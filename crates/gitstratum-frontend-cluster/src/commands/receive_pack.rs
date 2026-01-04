use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use gitstratum_core::{Blob, Commit, Object, Oid, Tree};
use tracing::warn;

use crate::error::{FrontendError, Result};
use crate::pack::assembly::PackReader;

#[derive(Debug, Clone)]
pub struct RefUpdate {
    pub ref_name: String,
    pub old_oid: Oid,
    pub new_oid: Oid,
    pub force: bool,
}

impl RefUpdate {
    pub fn new(ref_name: String, old_oid: Oid, new_oid: Oid) -> Self {
        Self {
            ref_name,
            old_oid,
            new_oid,
            force: false,
        }
    }

    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    pub fn is_create(&self) -> bool {
        self.old_oid.is_zero()
    }

    pub fn is_delete(&self) -> bool {
        self.new_oid.is_zero()
    }

    pub fn is_update(&self) -> bool {
        !self.is_create() && !self.is_delete()
    }

    pub fn parse(line: &str) -> Result<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(FrontendError::InvalidProtocol(
                "invalid ref update format".to_string(),
            ));
        }

        let old_oid = Oid::from_hex(parts[0])
            .map_err(|e| FrontendError::InvalidProtocol(format!("invalid old oid: {}", e)))?;
        let new_oid = Oid::from_hex(parts[1])
            .map_err(|e| FrontendError::InvalidProtocol(format!("invalid new oid: {}", e)))?;
        let ref_name = parts[2].to_string();

        Ok(Self::new(ref_name, old_oid, new_oid))
    }
}

#[derive(Debug, Clone)]
pub struct RefUpdateResult {
    pub ref_name: String,
    pub success: bool,
    pub error: Option<String>,
}

impl RefUpdateResult {
    pub fn ok(ref_name: String) -> Self {
        Self {
            ref_name,
            success: true,
            error: None,
        }
    }

    pub fn error(ref_name: String, error: String) -> Self {
        Self {
            ref_name,
            success: false,
            error: Some(error),
        }
    }

    pub fn to_report_line(&self) -> String {
        if self.success {
            format!("ok {}", self.ref_name)
        } else {
            format!(
                "ng {} {}",
                self.ref_name,
                self.error.as_deref().unwrap_or("unknown error")
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct PushResult {
    pub updates: Vec<RefUpdateResult>,
    pub objects_received: usize,
}

impl PushResult {
    pub fn new() -> Self {
        Self {
            updates: Vec::new(),
            objects_received: 0,
        }
    }

    pub fn all_successful(&self) -> bool {
        self.updates.iter().all(|u| u.success)
    }

    pub fn add_success(&mut self, ref_name: String) {
        self.updates.push(RefUpdateResult::ok(ref_name));
    }

    pub fn add_error(&mut self, ref_name: String, error: String) {
        self.updates.push(RefUpdateResult::error(ref_name, error));
    }

    pub fn to_report(&self) -> String {
        self.updates
            .iter()
            .map(|u| u.to_report_line())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Default for PushResult {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait ControlPlaneClient: Send + Sync {
    async fn acquire_ref_lock(
        &self,
        repo_id: &str,
        ref_name: &str,
        holder_id: &str,
        timeout_ms: u64,
    ) -> Result<String>;
    async fn release_ref_lock(&self, lock_id: &str) -> Result<()>;
}

#[async_trait]
pub trait MetadataWriter: Send + Sync {
    async fn put_commit(&self, repo_id: &str, commit: &Commit) -> Result<()>;
    async fn put_tree(&self, repo_id: &str, tree: &Tree) -> Result<()>;
    async fn update_ref(
        &self,
        repo_id: &str,
        ref_name: &str,
        old_oid: Oid,
        new_oid: Oid,
        force: bool,
    ) -> Result<bool>;
    async fn get_ref(&self, repo_id: &str, ref_name: &str) -> Result<Option<Oid>>;
}

#[async_trait]
pub trait ObjectWriter: Send + Sync {
    async fn put_blob(&self, blob: &Blob) -> Result<()>;
    async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()>;
}

pub struct GitReceivePack<C, M, O> {
    control_plane: Arc<C>,
    metadata_writer: Arc<M>,
    object_writer: Arc<O>,
    repo_id: String,
    holder_id: String,
    lock_timeout_ms: u64,
    max_pack_size: usize,
}

impl<C, M, O> GitReceivePack<C, M, O>
where
    C: ControlPlaneClient + 'static,
    M: MetadataWriter + 'static,
    O: ObjectWriter + 'static,
{
    pub fn new(
        control_plane: Arc<C>,
        metadata_writer: Arc<M>,
        object_writer: Arc<O>,
        repo_id: String,
        holder_id: String,
    ) -> Self {
        Self {
            control_plane,
            metadata_writer,
            object_writer,
            repo_id,
            holder_id,
            lock_timeout_ms: 30000,
            max_pack_size: 500 * 1024 * 1024,
        }
    }

    pub fn with_lock_timeout(mut self, timeout_ms: u64) -> Self {
        self.lock_timeout_ms = timeout_ms;
        self
    }

    pub fn with_max_pack_size(mut self, size: usize) -> Self {
        self.max_pack_size = size;
        self
    }

    pub async fn handle_push(
        &self,
        updates: Vec<RefUpdate>,
        pack_data: Bytes,
    ) -> Result<PushResult> {
        if pack_data.len() > self.max_pack_size {
            return Err(FrontendError::PackTooLarge {
                size: pack_data.len(),
                limit: self.max_pack_size,
            });
        }

        let objects = self.parse_and_validate_pack(pack_data)?;

        let mut locks = Vec::new();
        for update in &updates {
            let lock_id = self
                .control_plane
                .acquire_ref_lock(
                    &self.repo_id,
                    &update.ref_name,
                    &self.holder_id,
                    self.lock_timeout_ms,
                )
                .await?;
            locks.push((update.ref_name.clone(), lock_id));
        }

        let result = self.apply_push(&updates, objects).await;

        for (ref_name, lock_id) in locks {
            if let Err(e) = self.control_plane.release_ref_lock(&lock_id).await {
                warn!(ref_name = %ref_name, lock_id = %lock_id, error = %e, "failed to release ref lock");
            }
        }

        result
    }

    fn parse_and_validate_pack(&self, pack_data: Bytes) -> Result<HashMap<Oid, Object>> {
        if pack_data.len() < 12 {
            return Ok(HashMap::new());
        }

        let reader = PackReader::new(pack_data)?;
        let entries = reader.read_all()?;

        let mut objects = HashMap::new();
        for entry in entries {
            let obj = entry.into_object()?;
            objects.insert(*obj.oid(), obj);
        }

        Ok(objects)
    }

    async fn apply_push(
        &self,
        updates: &[RefUpdate],
        objects: HashMap<Oid, Object>,
    ) -> Result<PushResult> {
        let mut result = PushResult::new();
        result.objects_received = objects.len();

        self.store_objects(&objects).await?;

        for update in updates {
            match self.apply_ref_update(update).await {
                Ok(()) => result.add_success(update.ref_name.clone()),
                Err(e) => result.add_error(update.ref_name.clone(), e.to_string()),
            }
        }

        Ok(result)
    }

    async fn store_objects(&self, objects: &HashMap<Oid, Object>) -> Result<()> {
        let mut blobs = Vec::new();

        for obj in objects.values() {
            match obj {
                Object::Blob(b) => blobs.push(b.clone()),
                Object::Tree(t) => {
                    self.metadata_writer.put_tree(&self.repo_id, t).await?;
                }
                Object::Commit(c) => {
                    self.metadata_writer.put_commit(&self.repo_id, c).await?;
                }
            }
        }

        if !blobs.is_empty() {
            self.object_writer.put_blobs(blobs).await?;
        }

        Ok(())
    }

    async fn apply_ref_update(&self, update: &RefUpdate) -> Result<()> {
        if !update.force && update.is_update() {
            let current = self
                .metadata_writer
                .get_ref(&self.repo_id, &update.ref_name)
                .await?;
            if current != Some(update.old_oid) {
                return Err(FrontendError::RefUpdateRejected(format!(
                    "ref {} is at {:?}, expected {}",
                    update.ref_name, current, update.old_oid
                )));
            }
        }

        let success = self
            .metadata_writer
            .update_ref(
                &self.repo_id,
                &update.ref_name,
                update.old_oid,
                update.new_oid,
                update.force,
            )
            .await?;

        if !success {
            return Err(FrontendError::RefUpdateRejected(format!(
                "ref update for {} was rejected",
                update.ref_name
            )));
        }

        Ok(())
    }

    pub async fn validate_updates(&self, updates: &[RefUpdate]) -> Result<Vec<RefUpdateResult>> {
        let mut results = Vec::new();

        for update in updates {
            if update.is_delete() {
                results.push(RefUpdateResult::ok(update.ref_name.clone()));
                continue;
            }

            if !update.ref_name.starts_with("refs/") {
                results.push(RefUpdateResult::error(
                    update.ref_name.clone(),
                    "ref name must start with refs/".to_string(),
                ));
                continue;
            }

            if update.ref_name.contains("..") {
                results.push(RefUpdateResult::error(
                    update.ref_name.clone(),
                    "ref name cannot contain ..".to_string(),
                ));
                continue;
            }

            results.push(RefUpdateResult::ok(update.ref_name.clone()));
        }

        Ok(results)
    }
}

#[derive(Clone)]
pub struct ReceivePackCapabilities {
    pub report_status: bool,
    pub delete_refs: bool,
    pub ofs_delta: bool,
    pub push_options: bool,
    pub atomic: bool,
    pub quiet: bool,
}

impl ReceivePackCapabilities {
    pub fn new() -> Self {
        Self {
            report_status: false,
            delete_refs: false,
            ofs_delta: false,
            push_options: false,
            atomic: false,
            quiet: false,
        }
    }

    pub fn default_server() -> Self {
        Self {
            report_status: true,
            delete_refs: true,
            ofs_delta: true,
            push_options: true,
            atomic: true,
            quiet: false,
        }
    }

    pub fn parse(caps_str: &str) -> Self {
        let mut caps = Self::new();

        for cap in caps_str.split_whitespace() {
            match cap {
                "report-status" => caps.report_status = true,
                "delete-refs" => caps.delete_refs = true,
                "ofs-delta" => caps.ofs_delta = true,
                "push-options" => caps.push_options = true,
                "atomic" => caps.atomic = true,
                "quiet" => caps.quiet = true,
                _ => {}
            }
        }

        caps
    }
}

impl fmt::Display for ReceivePackCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut caps = Vec::new();

        if self.report_status {
            caps.push("report-status");
        }
        if self.delete_refs {
            caps.push("delete-refs");
        }
        if self.ofs_delta {
            caps.push("ofs-delta");
        }
        if self.push_options {
            caps.push("push-options");
        }
        if self.atomic {
            caps.push("atomic");
        }
        if self.quiet {
            caps.push("quiet");
        }

        write!(f, "{}", caps.join(" "))
    }
}

impl Default for ReceivePackCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

pub fn parse_push_command(lines: &[String]) -> Result<Vec<RefUpdate>> {
    let mut updates = Vec::new();

    for line in lines {
        let line = line.trim();
        if line.is_empty() || line == "0000" {
            continue;
        }

        if line.starts_with("push-option ") {
            continue;
        }

        updates.push(RefUpdate::parse(line)?);
    }

    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Signature;
    use std::sync::Mutex;

    struct MockControlPlane {
        locks: Mutex<HashMap<String, String>>,
        next_lock_id: Mutex<u64>,
    }

    impl MockControlPlane {
        fn new() -> Self {
            Self {
                locks: Mutex::new(HashMap::new()),
                next_lock_id: Mutex::new(1),
            }
        }
    }

    #[async_trait]
    impl ControlPlaneClient for MockControlPlane {
        async fn acquire_ref_lock(
            &self,
            _repo_id: &str,
            ref_name: &str,
            _holder_id: &str,
            _timeout_ms: u64,
        ) -> Result<String> {
            let mut id = self.next_lock_id.lock().unwrap();
            let lock_id = format!("lock-{}", *id);
            *id += 1;
            self.locks
                .lock()
                .unwrap()
                .insert(lock_id.clone(), ref_name.to_string());
            Ok(lock_id)
        }

        async fn release_ref_lock(&self, lock_id: &str) -> Result<()> {
            self.locks.lock().unwrap().remove(lock_id);
            Ok(())
        }
    }

    struct MockMetadataWriter {
        commits: Mutex<HashMap<Oid, Commit>>,
        trees: Mutex<HashMap<Oid, Tree>>,
        refs: Mutex<HashMap<String, Oid>>,
    }

    impl MockMetadataWriter {
        fn new() -> Self {
            Self {
                commits: Mutex::new(HashMap::new()),
                trees: Mutex::new(HashMap::new()),
                refs: Mutex::new(HashMap::new()),
            }
        }

        fn set_ref(&self, name: &str, oid: Oid) {
            self.refs.lock().unwrap().insert(name.to_string(), oid);
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl MetadataWriter for MockMetadataWriter {
        async fn put_commit(&self, _repo_id: &str, commit: &Commit) -> Result<()> {
            self.commits
                .lock()
                .unwrap()
                .insert(commit.oid, commit.clone());
            Ok(())
        }

        async fn put_tree(&self, _repo_id: &str, tree: &Tree) -> Result<()> {
            self.trees.lock().unwrap().insert(tree.oid, tree.clone());
            Ok(())
        }

        async fn update_ref(
            &self,
            _repo_id: &str,
            ref_name: &str,
            _old_oid: Oid,
            new_oid: Oid,
            _force: bool,
        ) -> Result<bool> {
            if new_oid.is_zero() {
                self.refs.lock().unwrap().remove(ref_name);
            } else {
                self.refs
                    .lock()
                    .unwrap()
                    .insert(ref_name.to_string(), new_oid);
            }
            Ok(true)
        }

        async fn get_ref(&self, _repo_id: &str, ref_name: &str) -> Result<Option<Oid>> {
            Ok(self.refs.lock().unwrap().get(ref_name).copied())
        }
    }

    struct MockObjectWriter {
        blobs: Mutex<HashMap<Oid, Blob>>,
    }

    impl MockObjectWriter {
        fn new() -> Self {
            Self {
                blobs: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl ObjectWriter for MockObjectWriter {
        async fn put_blob(&self, blob: &Blob) -> Result<()> {
            self.blobs.lock().unwrap().insert(blob.oid, blob.clone());
            Ok(())
        }

        async fn put_blobs(&self, blobs: Vec<Blob>) -> Result<()> {
            let mut store = self.blobs.lock().unwrap();
            for blob in blobs {
                store.insert(blob.oid, blob);
            }
            Ok(())
        }
    }

    fn create_test_pack() -> Bytes {
        use crate::pack::assembly::PackWriter;

        let mut writer = PackWriter::new();
        let blob = Blob::new(b"test content".to_vec());
        writer.add_object(&Object::Blob(blob)).unwrap();

        let tree = Tree::new(vec![]);
        writer.add_object(&Object::Tree(tree.clone())).unwrap();

        let author = Signature::new("Test", "test@example.com", 1704067200, "+0000");
        let commit = Commit::new(tree.oid, vec![], author.clone(), author, "Test commit");
        writer.add_object(&Object::Commit(commit)).unwrap();

        writer.build().unwrap()
    }

    #[test]
    fn test_ref_update_parse() {
        let old = Oid::hash(b"old");
        let new = Oid::hash(b"new");
        let line = format!("{} {} refs/heads/main", old, new);

        let update = RefUpdate::parse(&line).unwrap();
        assert_eq!(update.old_oid, old);
        assert_eq!(update.new_oid, new);
        assert_eq!(update.ref_name, "refs/heads/main");
    }

    #[test]
    fn test_ref_update_types() {
        let zero = Oid::ZERO;
        let oid = Oid::hash(b"test");

        let create = RefUpdate::new("refs/heads/new".to_string(), zero, oid);
        assert!(create.is_create());
        assert!(!create.is_delete());
        assert!(!create.is_update());

        let delete = RefUpdate::new("refs/heads/old".to_string(), oid, zero);
        assert!(!delete.is_create());
        assert!(delete.is_delete());
        assert!(!delete.is_update());

        let update = RefUpdate::new("refs/heads/main".to_string(), oid, Oid::hash(b"new"));
        assert!(!update.is_create());
        assert!(!update.is_delete());
        assert!(update.is_update());
    }

    #[test]
    fn test_ref_update_result_report() {
        let ok = RefUpdateResult::ok("refs/heads/main".to_string());
        assert_eq!(ok.to_report_line(), "ok refs/heads/main");

        let err = RefUpdateResult::error(
            "refs/heads/main".to_string(),
            "fast-forward required".to_string(),
        );
        assert_eq!(
            err.to_report_line(),
            "ng refs/heads/main fast-forward required"
        );
    }

    #[test]
    fn test_push_result() {
        let mut result = PushResult::new();
        result.add_success("refs/heads/main".to_string());
        result.add_error("refs/heads/feature".to_string(), "rejected".to_string());

        assert!(!result.all_successful());

        let report = result.to_report();
        assert!(report.contains("ok refs/heads/main"));
        assert!(report.contains("ng refs/heads/feature rejected"));
    }

    #[tokio::test]
    async fn test_handle_push() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata.clone(),
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let pack_data = create_test_pack();
        let new_oid = Oid::hash(b"new-commit");
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            Oid::ZERO,
            new_oid,
        )];

        let result = receive_pack.handle_push(updates, pack_data).await.unwrap();
        assert!(result.all_successful());
        assert!(result.objects_received > 0);
    }

    #[tokio::test]
    async fn test_handle_push_update_existing() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let old_oid = Oid::hash(b"old");
        metadata.set_ref("refs/heads/main", old_oid);

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let pack_data = create_test_pack();
        let new_oid = Oid::hash(b"new");
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            old_oid,
            new_oid,
        )];

        let result = receive_pack.handle_push(updates, pack_data).await.unwrap();
        assert!(result.all_successful());
    }

    #[tokio::test]
    async fn test_validate_updates() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let updates = vec![
            RefUpdate::new(
                "refs/heads/valid".to_string(),
                Oid::ZERO,
                Oid::hash(b"test"),
            ),
            RefUpdate::new("invalid-ref".to_string(), Oid::ZERO, Oid::hash(b"test")),
            RefUpdate::new(
                "refs/heads/bad..ref".to_string(),
                Oid::ZERO,
                Oid::hash(b"test"),
            ),
        ];

        let results = receive_pack.validate_updates(&updates).await.unwrap();
        assert!(results[0].success);
        assert!(!results[1].success);
        assert!(!results[2].success);
    }

    #[test]
    fn test_receive_pack_capabilities_parse() {
        let caps = ReceivePackCapabilities::parse("report-status delete-refs atomic");

        assert!(caps.report_status);
        assert!(caps.delete_refs);
        assert!(caps.atomic);
        assert!(!caps.push_options);
    }

    #[test]
    fn test_receive_pack_capabilities_to_string() {
        let mut caps = ReceivePackCapabilities::new();
        caps.report_status = true;
        caps.atomic = true;

        let s = caps.to_string();
        assert!(s.contains("report-status"));
        assert!(s.contains("atomic"));
    }

    #[test]
    fn test_parse_push_command() {
        let old = Oid::hash(b"old");
        let new = Oid::hash(b"new");

        let lines = vec![
            format!("{} {} refs/heads/main", old, new),
            format!("{} {} refs/heads/feature", Oid::ZERO, new),
            "push-option some-option".to_string(),
            "0000".to_string(),
        ];

        let updates = parse_push_command(&lines).unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].ref_name, "refs/heads/main");
        assert_eq!(updates[1].ref_name, "refs/heads/feature");
    }

    #[tokio::test]
    async fn test_pack_too_large() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        )
        .with_max_pack_size(10);

        let pack_data = create_test_pack();
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            Oid::ZERO,
            Oid::hash(b"test"),
        )];

        let result = receive_pack.handle_push(updates, pack_data).await;
        assert!(matches!(result, Err(FrontendError::PackTooLarge { .. })));
    }

    #[tokio::test]
    async fn test_with_options() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        )
        .with_lock_timeout(60000)
        .with_max_pack_size(1024 * 1024);

        assert_eq!(receive_pack.lock_timeout_ms, 60000);
        assert_eq!(receive_pack.max_pack_size, 1024 * 1024);
    }

    #[test]
    fn test_ref_update_with_force() {
        let update = RefUpdate::new("refs/heads/main".to_string(), Oid::ZERO, Oid::hash(b"test"))
            .with_force(true);

        assert!(update.force);
    }

    #[tokio::test]
    async fn test_empty_pack() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let empty_pack = Bytes::new();
        let new_oid = Oid::hash(b"test");
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            Oid::ZERO,
            new_oid,
        )];

        let result = receive_pack.handle_push(updates, empty_pack).await.unwrap();
        assert!(result.all_successful());
        assert_eq!(result.objects_received, 0);
    }

    #[test]
    fn test_ref_update_parse_invalid() {
        let result = RefUpdate::parse("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_push_result_default() {
        let result = PushResult::default();
        assert!(result.updates.is_empty());
        assert_eq!(result.objects_received, 0);
    }

    #[test]
    fn test_receive_pack_capabilities_default() {
        let caps = ReceivePackCapabilities::default();
        assert!(!caps.report_status);
        assert!(!caps.atomic);
    }

    #[test]
    fn test_receive_pack_capabilities_server() {
        let caps = ReceivePackCapabilities::default_server();
        assert!(caps.report_status);
        assert!(caps.delete_refs);
        assert!(caps.ofs_delta);
        assert!(caps.push_options);
        assert!(caps.atomic);
        assert!(!caps.quiet);
    }

    #[test]
    fn test_receive_pack_capabilities_to_string_all() {
        let mut caps = ReceivePackCapabilities::new();
        caps.report_status = true;
        caps.delete_refs = true;
        caps.ofs_delta = true;
        caps.push_options = true;
        caps.atomic = true;
        caps.quiet = true;

        let s = caps.to_string();
        assert!(s.contains("report-status"));
        assert!(s.contains("delete-refs"));
        assert!(s.contains("ofs-delta"));
        assert!(s.contains("push-options"));
        assert!(s.contains("atomic"));
        assert!(s.contains("quiet"));
    }

    #[test]
    fn test_receive_pack_capabilities_parse_quiet() {
        let caps = ReceivePackCapabilities::parse("quiet");
        assert!(caps.quiet);
    }

    #[tokio::test]
    async fn test_validate_updates_delete() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let updates = vec![RefUpdate::new(
            "refs/heads/to-delete".to_string(),
            Oid::hash(b"old"),
            Oid::ZERO,
        )];

        let results = receive_pack.validate_updates(&updates).await.unwrap();
        assert!(results[0].success);
    }

    #[test]
    fn test_receive_pack_capabilities_parse_unknown() {
        let caps = ReceivePackCapabilities::parse("unknown-capability foo-bar");
        assert!(!caps.report_status);
        assert!(!caps.atomic);
    }

    #[test]
    fn test_ref_update_result_error_unknown() {
        let err = RefUpdateResult::error("refs/heads/main".to_string(), String::new());
        assert!(!err.success);
        assert_eq!(err.to_report_line(), "ng refs/heads/main ");
    }

    struct MockMetadataWriterRejectUpdate {
        refs: Mutex<HashMap<String, Oid>>,
    }

    impl MockMetadataWriterRejectUpdate {
        fn new() -> Self {
            Self {
                refs: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    #[cfg_attr(coverage_nightly, coverage(off))]
    impl MetadataWriter for MockMetadataWriterRejectUpdate {
        async fn put_commit(&self, _repo_id: &str, _commit: &Commit) -> Result<()> {
            Ok(())
        }

        async fn put_tree(&self, _repo_id: &str, _tree: &Tree) -> Result<()> {
            Ok(())
        }

        async fn update_ref(
            &self,
            _repo_id: &str,
            _ref_name: &str,
            _old_oid: Oid,
            _new_oid: Oid,
            _force: bool,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn get_ref(&self, _repo_id: &str, ref_name: &str) -> Result<Option<Oid>> {
            Ok(self.refs.lock().unwrap().get(ref_name).copied())
        }
    }

    #[tokio::test]
    async fn test_handle_push_ref_update_rejected() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriterRejectUpdate::new());
        let objects = Arc::new(MockObjectWriter::new());

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let pack_data = Bytes::new();
        let new_oid = Oid::hash(b"new");
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            Oid::ZERO,
            new_oid,
        )];

        let result = receive_pack.handle_push(updates, pack_data).await.unwrap();
        assert!(!result.all_successful());
        assert!(result.updates[0]
            .error
            .as_ref()
            .unwrap()
            .contains("rejected"));
    }

    #[tokio::test]
    async fn test_handle_push_ref_wrong_old_oid() {
        let control_plane = Arc::new(MockControlPlane::new());
        let metadata = Arc::new(MockMetadataWriter::new());
        let objects = Arc::new(MockObjectWriter::new());

        let actual_oid = Oid::hash(b"actual");
        let wrong_old_oid = Oid::hash(b"wrong");
        let new_oid = Oid::hash(b"new");
        metadata.set_ref("refs/heads/main", actual_oid);

        let receive_pack = GitReceivePack::new(
            control_plane,
            metadata,
            objects,
            "test-repo".to_string(),
            "client-1".to_string(),
        );

        let pack_data = Bytes::new();
        let updates = vec![RefUpdate::new(
            "refs/heads/main".to_string(),
            wrong_old_oid,
            new_oid,
        )];

        let result = receive_pack.handle_push(updates, pack_data).await.unwrap();
        assert!(!result.all_successful());
        assert!(result.updates[0]
            .error
            .as_ref()
            .unwrap()
            .contains("expected"));
    }
}
