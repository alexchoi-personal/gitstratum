use bytes::Bytes;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::oid::Oid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    Blob,
    Tree,
    Commit,
    Tag,
}

impl ObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectType::Blob => "blob",
            ObjectType::Tree => "tree",
            ObjectType::Commit => "commit",
            ObjectType::Tag => "tag",
        }
    }
}

impl FromStr for ObjectType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "blob" => Ok(ObjectType::Blob),
            "tree" => Ok(ObjectType::Tree),
            "commit" => Ok(ObjectType::Commit),
            "tag" => Ok(ObjectType::Tag),
            _ => Err(Error::InvalidObjectType(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Object {
    Blob(Blob),
    Tree(Tree),
    Commit(Commit),
}

impl Object {
    pub fn object_type(&self) -> ObjectType {
        match self {
            Object::Blob(_) => ObjectType::Blob,
            Object::Tree(_) => ObjectType::Tree,
            Object::Commit(_) => ObjectType::Commit,
        }
    }

    pub fn oid(&self) -> &Oid {
        match self {
            Object::Blob(b) => &b.oid,
            Object::Tree(t) => &t.oid,
            Object::Commit(c) => &c.oid,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Object::Blob(b) => b.data.len(),
            Object::Tree(t) => t.entries.len() * 50,
            Object::Commit(c) => c.message.len() + 200,
        }
    }

    pub fn is_blob(&self) -> bool {
        matches!(self, Object::Blob(_))
    }

    pub fn is_tree(&self) -> bool {
        matches!(self, Object::Tree(_))
    }

    pub fn is_commit(&self) -> bool {
        matches!(self, Object::Commit(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    pub oid: Oid,
    pub data: Bytes,
}

impl Blob {
    pub fn new(data: impl Into<Bytes>) -> Self {
        let data = data.into();
        let oid = Oid::hash_object("blob", &data);
        Self { oid, data }
    }

    /// Creates a blob with a pre-computed OID without verification.
    ///
    /// # Safety
    /// The caller must ensure the OID matches the hash of the data.
    /// Use `with_oid_verified` if verification is needed.
    pub fn with_oid(oid: Oid, data: impl Into<Bytes>) -> Self {
        Self {
            oid,
            data: data.into(),
        }
    }

    /// Creates a blob with a pre-computed OID, verifying it matches the data.
    ///
    /// Returns an error if the computed OID doesn't match the expected OID.
    pub fn with_oid_verified(oid: Oid, data: impl Into<Bytes>) -> Result<Self> {
        let data = data.into();
        let computed = Oid::hash_object("blob", &data);
        if computed != oid {
            return Err(Error::OidMismatch {
                expected: oid.to_string(),
                computed: computed.to_string(),
            });
        }
        Ok(Self { oid, data })
    }

    pub fn compress(&self) -> Result<Bytes> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&self.data)
            .map_err(|e| Error::Compression(e.to_string()))?;
        let compressed = encoder
            .finish()
            .map_err(|e| Error::Compression(e.to_string()))?;
        Ok(Bytes::from(compressed))
    }

    pub fn decompress(oid: Oid, compressed: &[u8]) -> Result<Self> {
        let mut decoder = ZlibDecoder::new(compressed);
        let mut data = Vec::new();
        decoder
            .read_to_end(&mut data)
            .map_err(|e| Error::Decompression(e.to_string()))?;
        Ok(Self {
            oid,
            data: Bytes::from(data),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TreeEntryMode {
    File,
    Executable,
    Symlink,
    Directory,
    Submodule,
}

impl TreeEntryMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            TreeEntryMode::File => "100644",
            TreeEntryMode::Executable => "100755",
            TreeEntryMode::Symlink => "120000",
            TreeEntryMode::Directory => "040000",
            TreeEntryMode::Submodule => "160000",
        }
    }

    pub fn is_tree(&self) -> bool {
        matches!(self, TreeEntryMode::Directory)
    }
}

impl FromStr for TreeEntryMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "100644" => Ok(TreeEntryMode::File),
            "100755" => Ok(TreeEntryMode::Executable),
            "120000" => Ok(TreeEntryMode::Symlink),
            "040000" | "40000" => Ok(TreeEntryMode::Directory),
            "160000" => Ok(TreeEntryMode::Submodule),
            _ => Err(Error::InvalidObjectFormat(format!(
                "invalid tree entry mode: {}",
                s
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    pub mode: TreeEntryMode,
    pub name: String,
    pub oid: Oid,
}

impl TreeEntry {
    pub fn new(mode: TreeEntryMode, name: impl Into<String>, oid: Oid) -> Self {
        Self {
            mode,
            name: name.into(),
            oid,
        }
    }

    pub fn file(name: impl Into<String>, oid: Oid) -> Self {
        Self::new(TreeEntryMode::File, name, oid)
    }

    pub fn directory(name: impl Into<String>, oid: Oid) -> Self {
        Self::new(TreeEntryMode::Directory, name, oid)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tree {
    pub oid: Oid,
    pub entries: Vec<TreeEntry>,
}

impl Tree {
    pub fn new(entries: Vec<TreeEntry>) -> Self {
        let mut sorted = entries;
        sorted.sort_by(|a, b| a.name.cmp(&b.name));

        let mut data = Vec::new();
        for entry in &sorted {
            data.extend_from_slice(entry.mode.as_str().as_bytes());
            data.push(b' ');
            data.extend_from_slice(entry.name.as_bytes());
            data.push(0);
            data.extend_from_slice(entry.oid.as_bytes());
        }

        let oid = Oid::hash_object("tree", &data);
        Self {
            oid,
            entries: sorted,
        }
    }

    /// Creates a tree with a pre-computed OID without verification.
    ///
    /// # Safety
    /// The caller must ensure the OID matches the hash of the entries.
    /// Use `with_oid_verified` if verification is needed.
    pub fn with_oid(oid: Oid, entries: Vec<TreeEntry>) -> Self {
        Self { oid, entries }
    }

    /// Creates a tree with a pre-computed OID, verifying it matches the entries.
    ///
    /// Returns an error if the computed OID doesn't match the expected OID.
    pub fn with_oid_verified(oid: Oid, entries: Vec<TreeEntry>) -> Result<Self> {
        let mut sorted = entries;
        sorted.sort_by(|a, b| a.name.cmp(&b.name));

        let mut data = Vec::new();
        for entry in &sorted {
            data.extend_from_slice(entry.mode.as_str().as_bytes());
            data.push(b' ');
            data.extend_from_slice(entry.name.as_bytes());
            data.push(0);
            data.extend_from_slice(entry.oid.as_bytes());
        }

        let computed = Oid::hash_object("tree", &data);
        if computed != oid {
            return Err(Error::OidMismatch {
                expected: oid.to_string(),
                computed: computed.to_string(),
            });
        }
        Ok(Self {
            oid,
            entries: sorted,
        })
    }

    pub fn find(&self, name: &str) -> Option<&TreeEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    pub fn blobs(&self) -> impl Iterator<Item = &TreeEntry> {
        self.entries.iter().filter(|e| !e.mode.is_tree())
    }

    pub fn trees(&self) -> impl Iterator<Item = &TreeEntry> {
        self.entries.iter().filter(|e| e.mode.is_tree())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub timestamp: i64,
    pub timezone: String,
}

impl Signature {
    pub fn new(
        name: impl Into<String>,
        email: impl Into<String>,
        timestamp: i64,
        timezone: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            email: email.into(),
            timestamp,
            timezone: timezone.into(),
        }
    }
}

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} <{}> {} {}",
            self.name, self.email, self.timestamp, self.timezone
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub oid: Oid,
    pub tree: Oid,
    pub parents: Vec<Oid>,
    pub author: Signature,
    pub committer: Signature,
    pub message: String,
}

impl Commit {
    pub fn new(
        tree: Oid,
        parents: Vec<Oid>,
        author: Signature,
        committer: Signature,
        message: impl Into<String>,
    ) -> Self {
        let message = message.into();

        let mut data = Vec::new();
        data.extend_from_slice(format!("tree {}\n", tree).as_bytes());
        for parent in &parents {
            data.extend_from_slice(format!("parent {}\n", parent).as_bytes());
        }
        data.extend_from_slice(format!("author {}\n", author).as_bytes());
        data.extend_from_slice(format!("committer {}\n", committer).as_bytes());
        data.extend_from_slice(b"\n");
        data.extend_from_slice(message.as_bytes());

        let oid = Oid::hash_object("commit", &data);

        Self {
            oid,
            tree,
            parents,
            author,
            committer,
            message,
        }
    }

    /// Creates a commit with a pre-computed OID without verification.
    ///
    /// # Safety
    /// The caller must ensure the OID matches the hash of the commit data.
    /// Use `with_oid_verified` if verification is needed.
    pub fn with_oid(
        oid: Oid,
        tree: Oid,
        parents: Vec<Oid>,
        author: Signature,
        committer: Signature,
        message: impl Into<String>,
    ) -> Self {
        Self {
            oid,
            tree,
            parents,
            author,
            committer,
            message: message.into(),
        }
    }

    /// Creates a commit with a pre-computed OID, verifying it matches the commit data.
    ///
    /// Returns an error if the computed OID doesn't match the expected OID.
    pub fn with_oid_verified(
        oid: Oid,
        tree: Oid,
        parents: Vec<Oid>,
        author: Signature,
        committer: Signature,
        message: impl Into<String>,
    ) -> Result<Self> {
        let message = message.into();

        let mut data = Vec::new();
        data.extend_from_slice(format!("tree {}\n", tree).as_bytes());
        for parent in &parents {
            data.extend_from_slice(format!("parent {}\n", parent).as_bytes());
        }
        data.extend_from_slice(format!("author {}\n", author).as_bytes());
        data.extend_from_slice(format!("committer {}\n", committer).as_bytes());
        data.extend_from_slice(b"\n");
        data.extend_from_slice(message.as_bytes());

        let computed = Oid::hash_object("commit", &data);
        if computed != oid {
            return Err(Error::OidMismatch {
                expected: oid.to_string(),
                computed: computed.to_string(),
            });
        }

        Ok(Self {
            oid,
            tree,
            parents,
            author,
            committer,
            message,
        })
    }

    pub fn is_root(&self) -> bool {
        self.parents.is_empty()
    }

    pub fn is_merge(&self) -> bool {
        self.parents.len() > 1
    }

    pub fn first_parent(&self) -> Option<&Oid> {
        self.parents.first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_new() {
        let blob = Blob::new(b"hello world".to_vec());
        assert!(!blob.oid.is_zero());
        assert_eq!(blob.data.as_ref(), b"hello world");
    }

    #[test]
    fn test_blob_compress_decompress() {
        let original = Blob::new(b"hello world".to_vec());
        let compressed = original.compress().unwrap();
        let decompressed = Blob::decompress(original.oid, &compressed).unwrap();
        assert_eq!(original.data, decompressed.data);
    }

    #[test]
    fn test_tree_new() {
        let entries = vec![
            TreeEntry::file("README.md", Oid::hash(b"readme")),
            TreeEntry::directory("src", Oid::hash(b"src")),
        ];
        let tree = Tree::new(entries);
        assert!(!tree.oid.is_zero());
        assert_eq!(tree.entries.len(), 2);
        assert_eq!(tree.entries[0].name, "README.md");
    }

    #[test]
    fn test_tree_find() {
        let entries = vec![
            TreeEntry::file("README.md", Oid::hash(b"readme")),
            TreeEntry::file("main.rs", Oid::hash(b"main")),
        ];
        let tree = Tree::new(entries);

        assert!(tree.find("README.md").is_some());
        assert!(tree.find("main.rs").is_some());
        assert!(tree.find("nonexistent").is_none());
    }

    #[test]
    fn test_commit_new() {
        let tree = Oid::hash(b"tree");
        let author = Signature::new("Author", "author@example.com", 1704067200, "+0000");
        let committer = Signature::new("Committer", "committer@example.com", 1704067200, "+0000");

        let commit = Commit::new(tree, vec![], author, committer, "Initial commit");
        assert!(!commit.oid.is_zero());
        assert!(commit.is_root());
        assert!(!commit.is_merge());
    }

    #[test]
    fn test_commit_with_parents() {
        let tree = Oid::hash(b"tree");
        let parent1 = Oid::hash(b"parent1");
        let parent2 = Oid::hash(b"parent2");
        let author = Signature::new("Author", "author@example.com", 1704067200, "+0000");
        let committer = author.clone();

        let commit = Commit::new(
            tree,
            vec![parent1, parent2],
            author,
            committer,
            "Merge commit",
        );
        assert!(!commit.is_root());
        assert!(commit.is_merge());
        assert_eq!(commit.first_parent(), Some(&parent1));
    }

    #[test]
    fn test_object_type() {
        assert_eq!(ObjectType::Blob.as_str(), "blob");
        assert_eq!(ObjectType::Tree.as_str(), "tree");
        assert_eq!(ObjectType::Commit.as_str(), "commit");
        assert_eq!(ObjectType::Tag.as_str(), "tag");
    }

    #[test]
    fn test_tree_entry_mode() {
        assert_eq!(TreeEntryMode::File.as_str(), "100644");
        assert_eq!(TreeEntryMode::Executable.as_str(), "100755");
        assert_eq!(TreeEntryMode::Directory.as_str(), "040000");
        assert!(TreeEntryMode::Directory.is_tree());
        assert!(!TreeEntryMode::File.is_tree());
    }

    #[test]
    fn test_object_type_from_str() {
        assert_eq!(ObjectType::from_str("blob").unwrap(), ObjectType::Blob);
        assert_eq!(ObjectType::from_str("tree").unwrap(), ObjectType::Tree);
        assert_eq!(ObjectType::from_str("commit").unwrap(), ObjectType::Commit);
        assert_eq!(ObjectType::from_str("tag").unwrap(), ObjectType::Tag);
        assert!(ObjectType::from_str("invalid").is_err());
    }

    #[test]
    fn test_object_enum_methods() {
        let blob = Blob::new(b"content".to_vec());
        let obj_blob = Object::Blob(blob.clone());
        assert_eq!(obj_blob.object_type(), ObjectType::Blob);
        assert!(obj_blob.is_blob());
        assert!(!obj_blob.is_tree());
        assert!(!obj_blob.is_commit());
        assert_eq!(obj_blob.oid(), &blob.oid);
        assert_eq!(obj_blob.size(), 7);

        let entries = vec![TreeEntry::file("test.txt", Oid::hash(b"test"))];
        let tree = Tree::new(entries);
        let obj_tree = Object::Tree(tree.clone());
        assert_eq!(obj_tree.object_type(), ObjectType::Tree);
        assert!(!obj_tree.is_blob());
        assert!(obj_tree.is_tree());
        assert!(!obj_tree.is_commit());
        assert_eq!(obj_tree.oid(), &tree.oid);
        assert_eq!(obj_tree.size(), 50);

        let tree_oid = Oid::hash(b"tree");
        let author = Signature::new("Author", "a@example.com", 1704067200, "+0000");
        let committer = Signature::new("Committer", "c@example.com", 1704067200, "+0000");
        let commit = Commit::new(tree_oid, vec![], author, committer, "Test");
        let obj_commit = Object::Commit(commit.clone());
        assert_eq!(obj_commit.object_type(), ObjectType::Commit);
        assert!(!obj_commit.is_blob());
        assert!(!obj_commit.is_tree());
        assert!(obj_commit.is_commit());
        assert_eq!(obj_commit.oid(), &commit.oid);
        assert_eq!(obj_commit.size(), "Test".len() + 200);
    }

    #[test]
    fn test_tree_entry_mode_all_variants() {
        assert_eq!(TreeEntryMode::Symlink.as_str(), "120000");
        assert_eq!(TreeEntryMode::Submodule.as_str(), "160000");
        assert!(!TreeEntryMode::Symlink.is_tree());
        assert!(!TreeEntryMode::Executable.is_tree());
        assert!(!TreeEntryMode::Submodule.is_tree());
    }

    #[test]
    fn test_tree_entry_mode_from_str() {
        assert_eq!(
            TreeEntryMode::from_str("100644").unwrap(),
            TreeEntryMode::File
        );
        assert_eq!(
            TreeEntryMode::from_str("100755").unwrap(),
            TreeEntryMode::Executable
        );
        assert_eq!(
            TreeEntryMode::from_str("120000").unwrap(),
            TreeEntryMode::Symlink
        );
        assert_eq!(
            TreeEntryMode::from_str("040000").unwrap(),
            TreeEntryMode::Directory
        );
        assert_eq!(
            TreeEntryMode::from_str("40000").unwrap(),
            TreeEntryMode::Directory
        );
        assert_eq!(
            TreeEntryMode::from_str("160000").unwrap(),
            TreeEntryMode::Submodule
        );
        assert!(TreeEntryMode::from_str("999999").is_err());
    }

    #[test]
    fn test_tree_blobs_and_trees() {
        let entries = vec![
            TreeEntry::file("README.md", Oid::hash(b"readme")),
            TreeEntry::directory("src", Oid::hash(b"src")),
            TreeEntry::file("main.rs", Oid::hash(b"main")),
        ];
        let tree = Tree::new(entries);

        let blobs: Vec<_> = tree.blobs().collect();
        assert_eq!(blobs.len(), 2);
        assert!(blobs.iter().all(|e| !e.mode.is_tree()));

        let trees: Vec<_> = tree.trees().collect();
        assert_eq!(trees.len(), 1);
        assert!(trees.iter().all(|e| e.mode.is_tree()));
    }

    #[test]
    fn test_blob_with_oid() {
        let oid = Oid::hash(b"custom");
        let blob = Blob::with_oid(oid, b"data".to_vec());
        assert_eq!(blob.oid, oid);
        assert_eq!(blob.data.as_ref(), b"data");
    }

    #[test]
    fn test_tree_with_oid() {
        let oid = Oid::hash(b"tree");
        let entries = vec![TreeEntry::file("test.txt", Oid::hash(b"test"))];
        let tree = Tree::with_oid(oid, entries);
        assert_eq!(tree.oid, oid);
    }

    #[test]
    fn test_commit_with_oid() {
        let oid = Oid::hash(b"commit");
        let tree_oid = Oid::hash(b"tree");
        let author = Signature::new("Author", "a@example.com", 1704067200, "+0000");
        let committer = author.clone();
        let commit = Commit::with_oid(oid, tree_oid, vec![], author, committer, "Message");
        assert_eq!(commit.oid, oid);
        assert_eq!(commit.message, "Message");
    }

    #[test]
    fn test_tree_entry_new() {
        let oid = Oid::hash(b"test");
        let entry = TreeEntry::new(TreeEntryMode::Executable, "script.sh", oid);
        assert_eq!(entry.mode, TreeEntryMode::Executable);
        assert_eq!(entry.name, "script.sh");
        assert_eq!(entry.oid, oid);
    }

    #[test]
    fn test_signature_display() {
        let sig = Signature::new("John Doe", "john@example.com", 1704067200, "+0000");
        let display = format!("{}", sig);
        assert_eq!(display, "John Doe <john@example.com> 1704067200 +0000");
    }

    #[test]
    fn test_blob_decompress_error() {
        let invalid_compressed_data = b"this is not valid zlib data";
        let oid = Oid::hash(b"test");
        let result = Blob::decompress(oid, invalid_compressed_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_object_type_debug_clone_copy() {
        let blob_type = ObjectType::Blob;
        let copied1 = blob_type;
        let copied2 = blob_type;
        assert_eq!(copied1, copied2);
        assert!(format!("{:?}", blob_type).contains("Blob"));

        let tree_type = ObjectType::Tree;
        assert!(format!("{:?}", tree_type).contains("Tree"));

        let commit_type = ObjectType::Commit;
        assert!(format!("{:?}", commit_type).contains("Commit"));

        let tag_type = ObjectType::Tag;
        assert!(format!("{:?}", tag_type).contains("Tag"));
    }

    #[test]
    fn test_tree_entry_mode_debug_clone_copy() {
        let mode = TreeEntryMode::Executable;
        let copied1 = mode;
        let copied2 = mode;
        assert_eq!(copied1, copied2);
        assert!(format!("{:?}", mode).contains("Executable"));

        assert!(format!("{:?}", TreeEntryMode::Symlink).contains("Symlink"));
        assert!(format!("{:?}", TreeEntryMode::Submodule).contains("Submodule"));
    }

    #[test]
    fn test_blob_debug_clone() {
        let blob = Blob::new(b"test data".to_vec());
        let cloned = blob.clone();
        assert_eq!(blob.oid, cloned.oid);
        assert_eq!(blob.data, cloned.data);
        assert!(format!("{:?}", blob).contains("Blob"));
    }

    #[test]
    fn test_tree_debug_clone() {
        let entries = vec![TreeEntry::file("test.txt", Oid::hash(b"test"))];
        let tree = Tree::new(entries);
        let cloned = tree.clone();
        assert_eq!(tree.oid, cloned.oid);
        assert_eq!(tree.entries.len(), cloned.entries.len());
        assert!(format!("{:?}", tree).contains("Tree"));
    }

    #[test]
    fn test_commit_debug_clone() {
        let tree_oid = Oid::hash(b"tree");
        let author = Signature::new("Author", "a@test.com", 1704067200, "+0000");
        let committer = author.clone();
        let commit = Commit::new(tree_oid, vec![], author, committer, "Message");
        let cloned = commit.clone();
        assert_eq!(commit.oid, cloned.oid);
        assert_eq!(commit.message, cloned.message);
        assert!(format!("{:?}", commit).contains("Commit"));
    }

    #[test]
    fn test_signature_debug_clone() {
        let sig = Signature::new("Test Name", "test@test.com", 1704067200, "-0500");
        let cloned = sig.clone();
        assert_eq!(sig.name, cloned.name);
        assert_eq!(sig.email, cloned.email);
        assert_eq!(sig.timestamp, cloned.timestamp);
        assert_eq!(sig.timezone, cloned.timezone);
        assert!(format!("{:?}", sig).contains("Signature"));
    }

    #[test]
    fn test_tree_entry_debug_clone() {
        let entry = TreeEntry::file("test.txt", Oid::hash(b"content"));
        let cloned = entry.clone();
        assert_eq!(entry.name, cloned.name);
        assert_eq!(entry.oid, cloned.oid);
        assert!(format!("{:?}", entry).contains("TreeEntry"));
    }

    #[test]
    fn test_object_debug_clone() {
        let blob = Blob::new(b"content".to_vec());
        let obj = Object::Blob(blob);
        let cloned = obj.clone();
        assert!(cloned.is_blob());
        assert!(format!("{:?}", obj).contains("Blob"));
    }

    #[test]
    fn test_blob_compress_large_data() {
        let large_data = vec![b'x'; 10000];
        let blob = Blob::new(large_data.clone());
        let compressed = blob.compress().unwrap();
        assert!(compressed.len() < large_data.len());
        let decompressed = Blob::decompress(blob.oid, &compressed).unwrap();
        assert_eq!(blob.data, decompressed.data);
    }

    #[test]
    fn test_commit_first_parent_empty() {
        let tree_oid = Oid::hash(b"tree");
        let author = Signature::new("A", "a@test.com", 0, "+0000");
        let committer = author.clone();
        let commit = Commit::new(tree_oid, vec![], author, committer, "Root");
        assert!(commit.first_parent().is_none());
    }

    #[test]
    fn test_tree_empty() {
        let tree = Tree::new(vec![]);
        assert!(tree.entries.is_empty());
        assert!(tree.find("anything").is_none());
        assert_eq!(tree.blobs().count(), 0);
        assert_eq!(tree.trees().count(), 0);
    }

    #[test]
    fn test_blob_empty() {
        let blob = Blob::new(Vec::new());
        assert!(blob.data.is_empty());
        let compressed = blob.compress().unwrap();
        let decompressed = Blob::decompress(blob.oid, &compressed).unwrap();
        assert!(decompressed.data.is_empty());
    }

    #[test]
    fn test_tree_entry_executable() {
        let entry = TreeEntry::new(TreeEntryMode::Executable, "script.sh", Oid::hash(b"script"));
        assert_eq!(entry.mode, TreeEntryMode::Executable);
        assert_eq!(entry.name, "script.sh");
    }

    #[test]
    fn test_tree_entry_symlink() {
        let entry = TreeEntry::new(TreeEntryMode::Symlink, "link", Oid::hash(b"target"));
        assert_eq!(entry.mode, TreeEntryMode::Symlink);
        assert!(!entry.mode.is_tree());
    }

    #[test]
    fn test_tree_entry_submodule() {
        let entry = TreeEntry::new(TreeEntryMode::Submodule, "submod", Oid::hash(b"commit"));
        assert_eq!(entry.mode, TreeEntryMode::Submodule);
        assert!(!entry.mode.is_tree());
    }

    #[test]
    fn test_commit_single_parent() {
        let tree_oid = Oid::hash(b"tree");
        let parent = Oid::hash(b"parent");
        let author = Signature::new("A", "a@test.com", 0, "+0000");
        let committer = author.clone();
        let commit = Commit::new(tree_oid, vec![parent], author, committer, "Child");
        assert!(!commit.is_root());
        assert!(!commit.is_merge());
        assert_eq!(commit.first_parent(), Some(&parent));
    }

    #[test]
    fn test_blob_with_oid_verified_success() {
        let data = b"test blob content";
        let expected_oid = Oid::hash_object("blob", data);
        let blob = Blob::with_oid_verified(expected_oid, data.to_vec()).unwrap();
        assert_eq!(blob.oid, expected_oid);
        assert_eq!(blob.data.as_ref(), data);
    }

    #[test]
    fn test_blob_with_oid_verified_mismatch() {
        let data = b"test blob content";
        let wrong_oid = Oid::hash(b"wrong");
        let result = Blob::with_oid_verified(wrong_oid, data.to_vec());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::OidMismatch { .. }));
    }

    #[test]
    fn test_tree_with_oid_verified_success() {
        let entries = vec![
            TreeEntry::file("README.md", Oid::hash(b"readme")),
            TreeEntry::directory("src", Oid::hash(b"src")),
        ];
        let tree_from_new = Tree::new(entries.clone());
        let tree = Tree::with_oid_verified(tree_from_new.oid, entries).unwrap();
        assert_eq!(tree.oid, tree_from_new.oid);
    }

    #[test]
    fn test_tree_with_oid_verified_mismatch() {
        let entries = vec![TreeEntry::file("test.txt", Oid::hash(b"test"))];
        let wrong_oid = Oid::hash(b"wrong");
        let result = Tree::with_oid_verified(wrong_oid, entries);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::OidMismatch { .. }));
    }

    #[test]
    fn test_commit_with_oid_verified_success() {
        let tree_oid = Oid::hash(b"tree");
        let author = Signature::new("Author", "a@test.com", 1704067200, "+0000");
        let committer = author.clone();
        let commit_from_new = Commit::new(
            tree_oid,
            vec![],
            author.clone(),
            committer.clone(),
            "Test message",
        );
        let commit = Commit::with_oid_verified(
            commit_from_new.oid,
            tree_oid,
            vec![],
            author,
            committer,
            "Test message",
        )
        .unwrap();
        assert_eq!(commit.oid, commit_from_new.oid);
    }

    #[test]
    fn test_commit_with_oid_verified_mismatch() {
        let tree_oid = Oid::hash(b"tree");
        let author = Signature::new("Author", "a@test.com", 1704067200, "+0000");
        let committer = author.clone();
        let wrong_oid = Oid::hash(b"wrong");
        let result =
            Commit::with_oid_verified(wrong_oid, tree_oid, vec![], author, committer, "Message");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::OidMismatch { .. }));
    }
}
