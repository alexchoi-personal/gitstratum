use bytes::{BufMut, Bytes, BytesMut};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use gitstratum_core::{
    Blob, Commit, Object, ObjectType, Oid, Signature, Tree, TreeEntry, TreeEntryMode,
};
use rayon::prelude::*;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::str::FromStr;

use crate::error::{FrontendError, Result};

const PACK_SIGNATURE: &[u8; 4] = b"PACK";
const PACK_VERSION: u32 = 2;

const OBJ_COMMIT: u8 = 1;
const OBJ_TREE: u8 = 2;
const OBJ_BLOB: u8 = 3;
const OBJ_TAG: u8 = 4;
const OBJ_OFS_DELTA: u8 = 6;
const OBJ_REF_DELTA: u8 = 7;

const MAX_BASE_OBJECTS: usize = 1_000_000;

#[derive(Debug, Clone)]
pub struct PackEntry {
    pub oid: Oid,
    pub object_type: ObjectType,
    pub data: Bytes,
}

impl PackEntry {
    pub fn new(oid: Oid, object_type: ObjectType, data: Bytes) -> Self {
        Self {
            oid,
            object_type,
            data,
        }
    }

    pub fn from_object(obj: &Object) -> Result<Self> {
        let (oid, object_type, data) = match obj {
            Object::Blob(b) => (b.oid, ObjectType::Blob, b.data.clone()),
            Object::Tree(t) => {
                let data = serialize_tree(t);
                (t.oid, ObjectType::Tree, data)
            }
            Object::Commit(c) => {
                let data = serialize_commit(c);
                (c.oid, ObjectType::Commit, data)
            }
        };
        Ok(Self {
            oid,
            object_type,
            data,
        })
    }

    pub fn into_object(self) -> Result<Object> {
        match self.object_type {
            ObjectType::Blob => Ok(Object::Blob(Blob::with_oid(self.oid, self.data))),
            ObjectType::Tree => {
                let entries = parse_tree_entries(&self.data)?;
                Ok(Object::Tree(Tree::with_oid(self.oid, entries)))
            }
            ObjectType::Commit => {
                let commit = parse_commit(self.oid, &self.data)?;
                Ok(Object::Commit(commit))
            }
            ObjectType::Tag => Err(FrontendError::InvalidObjectType {
                expected: "blob, tree, or commit".to_string(),
                actual: "tag".to_string(),
            }),
        }
    }
}

fn serialize_tree(tree: &Tree) -> Bytes {
    let estimated_size = tree.entries.iter().map(|e| 7 + e.name.len() + 1 + 32).sum();
    let mut data = Vec::with_capacity(estimated_size);
    for entry in &tree.entries {
        data.extend_from_slice(entry.mode.as_str().as_bytes());
        data.push(b' ');
        data.extend_from_slice(entry.name.as_bytes());
        data.push(0);
        data.extend_from_slice(entry.oid.as_bytes());
    }
    Bytes::from(data)
}

fn serialize_commit(commit: &Commit) -> Bytes {
    use std::io::Write;
    let estimated_size = 5
        + 64
        + 1
        + commit.parents.len() * (7 + 64 + 1)
        + 7
        + 100
        + 10
        + 100
        + 1
        + commit.message.len();
    let mut data = Vec::with_capacity(estimated_size);
    writeln!(data, "tree {}", commit.tree).ok();
    for parent in &commit.parents {
        writeln!(data, "parent {}", parent).ok();
    }
    writeln!(data, "author {}", commit.author).ok();
    writeln!(data, "committer {}", commit.committer).ok();
    data.push(b'\n');
    data.extend_from_slice(commit.message.as_bytes());
    Bytes::from(data)
}

fn parse_tree_entries(data: &[u8]) -> Result<Vec<TreeEntry>> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let space_pos = data[pos..].iter().position(|&b| b == b' ').ok_or_else(|| {
            FrontendError::InvalidPackFormat("missing space in tree entry".to_string())
        })?;

        let mode_str = std::str::from_utf8(&data[pos..pos + space_pos])
            .map_err(|e| FrontendError::InvalidPackFormat(e.to_string()))?;
        let mode = TreeEntryMode::from_str(mode_str)?;
        pos += space_pos + 1;

        let null_pos = data[pos..].iter().position(|&b| b == 0).ok_or_else(|| {
            FrontendError::InvalidPackFormat("missing null in tree entry".to_string())
        })?;

        let name = std::str::from_utf8(&data[pos..pos + null_pos])
            .map_err(|e| FrontendError::InvalidPackFormat(e.to_string()))?
            .to_string();
        pos += null_pos + 1;

        if pos + 32 > data.len() {
            return Err(FrontendError::InvalidPackFormat(
                "truncated tree entry oid".to_string(),
            ));
        }

        let oid = Oid::from_slice(&data[pos..pos + 32])?;
        pos += 32;

        entries.push(TreeEntry::new(mode, name, oid));
    }

    Ok(entries)
}

fn parse_commit(oid: Oid, data: &[u8]) -> Result<Commit> {
    let text =
        std::str::from_utf8(data).map_err(|e| FrontendError::InvalidPackFormat(e.to_string()))?;

    let mut tree: Option<Oid> = None;
    let mut parents = Vec::new();
    let mut author: Option<Signature> = None;
    let mut committer: Option<Signature> = None;
    let mut in_message = false;
    let mut message = String::new();

    for line in text.lines() {
        if in_message {
            if !message.is_empty() {
                message.push('\n');
            }
            message.push_str(line);
            continue;
        }

        if line.is_empty() {
            in_message = true;
            continue;
        }

        if let Some(rest) = line.strip_prefix("tree ") {
            tree = Some(Oid::from_hex(rest)?);
        } else if let Some(rest) = line.strip_prefix("parent ") {
            parents.push(Oid::from_hex(rest)?);
        } else if let Some(rest) = line.strip_prefix("author ") {
            author = Some(parse_signature(rest)?);
        } else if let Some(rest) = line.strip_prefix("committer ") {
            committer = Some(parse_signature(rest)?);
        }
    }

    Ok(Commit::with_oid(
        oid,
        tree.ok_or_else(|| FrontendError::InvalidPackFormat("missing tree".to_string()))?,
        parents,
        author.ok_or_else(|| FrontendError::InvalidPackFormat("missing author".to_string()))?,
        committer
            .ok_or_else(|| FrontendError::InvalidPackFormat("missing committer".to_string()))?,
        message,
    ))
}

fn parse_signature(s: &str) -> Result<Signature> {
    let email_start = s
        .find('<')
        .ok_or_else(|| FrontendError::InvalidPackFormat("missing < in signature".to_string()))?;
    let email_end = s
        .find('>')
        .ok_or_else(|| FrontendError::InvalidPackFormat("missing > in signature".to_string()))?;

    let name = s[..email_start].trim().to_string();
    let email = s[email_start + 1..email_end].to_string();

    let rest = s[email_end + 1..].trim();
    let parts: Vec<&str> = rest.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(FrontendError::InvalidPackFormat(
            "invalid signature timestamp".to_string(),
        ));
    }

    let timestamp: i64 = parts[0]
        .parse()
        .map_err(|_| FrontendError::InvalidPackFormat("invalid timestamp".to_string()))?;
    let timezone = parts[1].to_string();

    Ok(Signature::new(name, email, timestamp, timezone))
}

pub struct PackWriter {
    entries: Vec<PackEntry>,
    include_header: bool,
}

impl PackWriter {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            include_header: true,
        }
    }

    pub fn without_header() -> Self {
        Self {
            entries: Vec::new(),
            include_header: false,
        }
    }

    pub fn add_entry(&mut self, entry: PackEntry) {
        self.entries.push(entry);
    }

    pub fn add_object(&mut self, obj: &Object) -> Result<()> {
        let entry = PackEntry::from_object(obj)?;
        self.entries.push(entry);
        Ok(())
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn build(self) -> Result<Bytes> {
        let compressed_entries: Result<Vec<_>> = self
            .entries
            .par_iter()
            .map(|entry| {
                let type_byte = match entry.object_type {
                    ObjectType::Commit => OBJ_COMMIT,
                    ObjectType::Tree => OBJ_TREE,
                    ObjectType::Blob => OBJ_BLOB,
                    ObjectType::Tag => OBJ_TAG,
                };
                let compressed = compress_data(&entry.data)?;
                Ok((type_byte, entry.data.len(), compressed))
            })
            .collect();
        let compressed_entries = compressed_entries?;

        let mut buf = BytesMut::new();
        if self.include_header {
            buf.put_slice(PACK_SIGNATURE);
            buf.put_u32(PACK_VERSION);
            buf.put_u32(self.entries.len() as u32);
        }

        for (type_byte, uncompressed_size, compressed) in compressed_entries {
            write_object_header(&mut buf, type_byte, uncompressed_size);
            buf.put_slice(&compressed);
        }

        Ok(buf.freeze())
    }

    pub fn build_streaming<W: Write>(self, writer: &mut W) -> Result<()> {
        let compressed_entries: Result<Vec<_>> = self
            .entries
            .par_iter()
            .map(|entry| {
                let type_byte = match entry.object_type {
                    ObjectType::Commit => OBJ_COMMIT,
                    ObjectType::Tree => OBJ_TREE,
                    ObjectType::Blob => OBJ_BLOB,
                    ObjectType::Tag => OBJ_TAG,
                };
                let compressed = compress_data(&entry.data)?;
                Ok((type_byte, entry.data.len(), compressed))
            })
            .collect();
        let compressed_entries = compressed_entries?;

        if self.include_header {
            writer.write_all(PACK_SIGNATURE)?;
            writer.write_all(&PACK_VERSION.to_be_bytes())?;
            writer.write_all(&(self.entries.len() as u32).to_be_bytes())?;
        }

        for (type_byte, uncompressed_size, compressed) in compressed_entries {
            let mut header_buf = BytesMut::new();
            write_object_header(&mut header_buf, type_byte, uncompressed_size);
            writer.write_all(&header_buf)?;
            writer.write_all(&compressed)?;
        }

        Ok(())
    }
}

impl Default for PackWriter {
    fn default() -> Self {
        Self::new()
    }
}

fn compress_data(data: &[u8]) -> Result<Bytes> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| FrontendError::Compression(e.to_string()))?;
    let compressed = encoder
        .finish()
        .map_err(|e| FrontendError::Compression(e.to_string()))?;
    Ok(Bytes::from(compressed))
}

fn write_object_header(buf: &mut BytesMut, type_byte: u8, size: usize) {
    let mut size = size;
    let mut byte = (type_byte << 4) | ((size as u8) & 0x0f);
    size >>= 4;

    while size > 0 {
        buf.put_u8(byte | 0x80);
        byte = (size as u8) & 0x7f;
        size >>= 7;
    }

    buf.put_u8(byte);
}

pub struct PackReader {
    data: Bytes,
    pos: usize,
    object_count: u32,
    objects_read: u32,
    base_objects: HashMap<Oid, PackEntry>,
}

impl PackReader {
    pub fn new(data: Bytes) -> Result<Self> {
        if data.len() < 12 {
            return Err(FrontendError::InvalidPackFormat(
                "pack too small".to_string(),
            ));
        }

        if &data[0..4] != PACK_SIGNATURE {
            return Err(FrontendError::InvalidPackFormat(
                "invalid pack signature".to_string(),
            ));
        }

        let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        if version != PACK_VERSION {
            return Err(FrontendError::InvalidPackFormat(format!(
                "unsupported pack version: {}",
                version
            )));
        }

        let object_count = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

        Ok(Self {
            data,
            pos: 12,
            object_count,
            objects_read: 0,
            base_objects: HashMap::new(),
        })
    }

    pub fn from_raw(data: Bytes, object_count: u32) -> Self {
        Self {
            data,
            pos: 0,
            object_count,
            objects_read: 0,
            base_objects: HashMap::new(),
        }
    }

    pub fn object_count(&self) -> u32 {
        self.object_count
    }

    pub fn objects_remaining(&self) -> u32 {
        self.object_count.saturating_sub(self.objects_read)
    }

    pub fn read_next(&mut self) -> Result<Option<PackEntry>> {
        if self.objects_read >= self.object_count {
            return Ok(None);
        }

        if self.pos >= self.data.len() {
            return Err(FrontendError::UnexpectedEndOfStream);
        }

        let (type_byte, size) = self.read_object_header()?;
        let entry = match type_byte {
            OBJ_COMMIT | OBJ_TREE | OBJ_BLOB | OBJ_TAG => {
                let decompressed = self.decompress_object(size)?;
                let object_type = match type_byte {
                    OBJ_COMMIT => ObjectType::Commit,
                    OBJ_TREE => ObjectType::Tree,
                    OBJ_BLOB => ObjectType::Blob,
                    OBJ_TAG => ObjectType::Tag,
                    _ => unreachable!(),
                };
                let oid = Oid::hash_object(object_type.as_str(), &decompressed);
                PackEntry::new(oid, object_type, Bytes::from(decompressed))
            }
            OBJ_OFS_DELTA => {
                let _offset = self.read_offset_delta()?;
                let _decompressed = self.decompress_object(size)?;
                return Err(FrontendError::InvalidPackFormat(
                    "offset delta not yet supported".to_string(),
                ));
            }
            OBJ_REF_DELTA => {
                let base_oid = self.read_ref_delta()?;
                let delta_data = self.decompress_object(size)?;
                self.apply_delta(&base_oid, &delta_data)?
            }
            _ => {
                return Err(FrontendError::InvalidPackFormat(format!(
                    "unknown object type: {}",
                    type_byte
                )));
            }
        };

        self.objects_read += 1;
        if self.base_objects.len() < MAX_BASE_OBJECTS {
            self.base_objects.insert(entry.oid, entry.clone());
        }
        Ok(Some(entry))
    }

    fn read_object_header(&mut self) -> Result<(u8, usize)> {
        if self.pos >= self.data.len() {
            return Err(FrontendError::UnexpectedEndOfStream);
        }

        let first_byte = self.data[self.pos];
        self.pos += 1;

        let type_byte = (first_byte >> 4) & 0x07;
        let mut size = (first_byte & 0x0f) as usize;
        let mut shift = 4;

        let mut byte = first_byte;
        while byte & 0x80 != 0 {
            if self.pos >= self.data.len() {
                return Err(FrontendError::UnexpectedEndOfStream);
            }
            byte = self.data[self.pos];
            self.pos += 1;
            size |= ((byte & 0x7f) as usize) << shift;
            shift += 7;
        }

        Ok((type_byte, size))
    }

    fn decompress_object(&mut self, expected_size: usize) -> Result<Vec<u8>> {
        let compressed_start = self.pos;
        let remaining = &self.data[self.pos..];

        let mut decoder = ZlibDecoder::new(remaining);
        let mut decompressed = Vec::with_capacity(expected_size);
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| FrontendError::Decompression(e.to_string()))?;

        let consumed = decoder.total_in() as usize;
        self.pos = compressed_start + consumed;

        if decompressed.len() != expected_size {
            return Err(FrontendError::InvalidPackFormat(format!(
                "decompressed size mismatch: expected {}, got {}",
                expected_size,
                decompressed.len()
            )));
        }

        Ok(decompressed)
    }

    fn read_offset_delta(&mut self) -> Result<u64> {
        if self.pos >= self.data.len() {
            return Err(FrontendError::UnexpectedEndOfStream);
        }

        let mut byte = self.data[self.pos];
        self.pos += 1;
        let mut offset = (byte & 0x7f) as u64;

        while byte & 0x80 != 0 {
            if self.pos >= self.data.len() {
                return Err(FrontendError::UnexpectedEndOfStream);
            }
            byte = self.data[self.pos];
            self.pos += 1;
            offset = ((offset + 1) << 7) | ((byte & 0x7f) as u64);
        }

        Ok(offset)
    }

    fn read_ref_delta(&mut self) -> Result<Oid> {
        if self.pos + 32 > self.data.len() {
            return Err(FrontendError::UnexpectedEndOfStream);
        }

        let oid = Oid::from_slice(&self.data[self.pos..self.pos + 32])?;
        self.pos += 32;
        Ok(oid)
    }

    fn apply_delta(&self, base_oid: &Oid, delta_data: &[u8]) -> Result<PackEntry> {
        let base = self
            .base_objects
            .get(base_oid)
            .ok_or_else(|| FrontendError::ObjectNotFound(base_oid.to_string()))?;

        let result = apply_delta_instructions(&base.data, delta_data)?;
        let oid = Oid::hash_object(base.object_type.as_str(), &result);

        Ok(PackEntry::new(oid, base.object_type, Bytes::from(result)))
    }

    pub fn read_all(mut self) -> Result<Vec<PackEntry>> {
        let mut entries = Vec::with_capacity(self.object_count as usize);
        while let Some(entry) = self.read_next()? {
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn apply_delta_instructions(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let mut pos = 0;

    let (_base_size, consumed) = read_delta_size(&delta[pos..])?;
    pos += consumed;

    let (result_size, consumed) = read_delta_size(&delta[pos..])?;
    pos += consumed;

    let mut result = Vec::with_capacity(result_size);

    while pos < delta.len() {
        let cmd = delta[pos];
        pos += 1;

        if cmd & 0x80 != 0 {
            let mut offset: usize = 0;
            let mut size: usize = 0;

            if cmd & 0x01 != 0 {
                offset = delta[pos] as usize;
                pos += 1;
            }
            if cmd & 0x02 != 0 {
                offset |= (delta[pos] as usize) << 8;
                pos += 1;
            }
            if cmd & 0x04 != 0 {
                offset |= (delta[pos] as usize) << 16;
                pos += 1;
            }
            if cmd & 0x08 != 0 {
                offset |= (delta[pos] as usize) << 24;
                pos += 1;
            }

            if cmd & 0x10 != 0 {
                size = delta[pos] as usize;
                pos += 1;
            }
            if cmd & 0x20 != 0 {
                size |= (delta[pos] as usize) << 8;
                pos += 1;
            }
            if cmd & 0x40 != 0 {
                size |= (delta[pos] as usize) << 16;
                pos += 1;
            }

            if size == 0 {
                size = 0x10000;
            }

            if offset + size > base.len() {
                return Err(FrontendError::InvalidPackFormat(
                    "delta copy out of bounds".to_string(),
                ));
            }

            result.extend_from_slice(&base[offset..offset + size]);
        } else if cmd != 0 {
            let size = cmd as usize;
            if pos + size > delta.len() {
                return Err(FrontendError::InvalidPackFormat(
                    "delta insert out of bounds".to_string(),
                ));
            }
            result.extend_from_slice(&delta[pos..pos + size]);
            pos += size;
        } else {
            return Err(FrontendError::InvalidPackFormat(
                "invalid delta command".to_string(),
            ));
        }
    }

    if result.len() != result_size {
        return Err(FrontendError::InvalidPackFormat(format!(
            "delta result size mismatch: expected {}, got {}",
            result_size,
            result.len()
        )));
    }

    Ok(result)
}

fn read_delta_size(data: &[u8]) -> Result<(usize, usize)> {
    let mut size: usize = 0;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= data.len() {
            return Err(FrontendError::UnexpectedEndOfStream);
        }

        let byte = data[pos];
        pos += 1;
        size |= ((byte & 0x7f) as usize) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }
    }

    Ok((size, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_delta_size(size: usize) -> Vec<u8> {
        let mut result = Vec::new();
        let mut size = size;
        loop {
            let mut byte = (size & 0x7f) as u8;
            size >>= 7;
            if size > 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if size == 0 {
                break;
            }
        }
        result
    }

    #[test]
    fn test_pack_write_read_roundtrip_all_object_types() {
        let blob1 = Blob::new(b"hello world".to_vec());
        let blob1_oid = blob1.oid;
        let blob2 = Blob::new(b"test content for roundtrip verification".to_vec());
        let blob2_oid = blob2.oid;
        let large_blob = Blob::new(vec![0u8; 256]);
        let large_blob_oid = large_blob.oid;

        let tree_entries = vec![
            TreeEntry::file("README.md", Oid::hash(b"readme")),
            TreeEntry::file("main.rs", Oid::hash(b"main")),
            TreeEntry::directory("src", Oid::hash(b"src")),
            TreeEntry::new(TreeEntryMode::Executable, "script.sh", Oid::hash(b"script")),
        ];
        let tree = Tree::new(tree_entries.clone());
        let tree_oid = tree.oid;

        let tree2 = Tree::new(vec![
            TreeEntry::file("file1.txt", blob1.oid),
            TreeEntry::file("file2.txt", blob2.oid),
        ]);

        let parent_commit_oid = Oid::hash(b"parent");
        let author = Signature::new("Author Name", "author@example.com", 1704067200, "+0530");
        let committer = Signature::new(
            "Committer Name",
            "committer@example.com",
            1704067300,
            "-0800",
        );
        let commit = Commit::new(
            tree_oid,
            vec![parent_commit_oid],
            author,
            committer,
            "Test commit message\nWith multiple lines",
        );

        let author2 = Signature::new("Test", "test@example.com", 1234567890, "+0000");
        let commit2 = Commit::new(
            tree.oid,
            vec![],
            author2.clone(),
            author2.clone(),
            "Initial commit",
        );

        let tag_oid = Oid::hash(b"tag");
        let tag_entry =
            PackEntry::new(tag_oid, ObjectType::Tag, Bytes::from_static(b"tag content"));

        let mut writer = PackWriter::new();
        writer.add_object(&Object::Blob(blob1.clone())).unwrap();
        writer.add_object(&Object::Blob(blob2.clone())).unwrap();
        writer
            .add_object(&Object::Blob(large_blob.clone()))
            .unwrap();
        writer.add_object(&Object::Tree(tree.clone())).unwrap();
        writer.add_object(&Object::Tree(tree2.clone())).unwrap();
        writer.add_object(&Object::Commit(commit.clone())).unwrap();
        writer.add_object(&Object::Commit(commit2.clone())).unwrap();
        writer.add_entry(tag_entry.clone());
        assert_eq!(writer.entry_count(), 8);

        let pack_data = writer.build().unwrap();
        assert_eq!(&pack_data[0..4], PACK_SIGNATURE);
        assert_eq!(
            u32::from_be_bytes([pack_data[4], pack_data[5], pack_data[6], pack_data[7]]),
            PACK_VERSION
        );
        assert_eq!(
            u32::from_be_bytes([pack_data[8], pack_data[9], pack_data[10], pack_data[11]]),
            8
        );

        let mut reader = PackReader::new(pack_data.clone()).unwrap();
        assert_eq!(reader.object_count(), 8);
        assert_eq!(reader.objects_remaining(), 8);

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.oid, blob1_oid);
        assert_eq!(entry.object_type, ObjectType::Blob);
        assert_eq!(entry.data.as_ref(), b"hello world");
        let obj = entry.into_object().unwrap();
        match obj {
            Object::Blob(b) => assert_eq!(b.data, blob1.data),
            _ => panic!("expected blob"),
        }

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.oid, blob2_oid);
        assert_eq!(
            entry.data.as_ref(),
            b"test content for roundtrip verification"
        );

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.oid, large_blob_oid);
        assert_eq!(entry.data.len(), 256);

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.oid, tree_oid);
        assert_eq!(entry.object_type, ObjectType::Tree);
        let obj = entry.into_object().unwrap();
        match obj {
            Object::Tree(t) => {
                assert_eq!(t.entries.len(), 4);
                assert_eq!(t.entries[0].name, "README.md");
                assert_eq!(t.entries[0].mode, TreeEntryMode::File);
                assert_eq!(t.entries[1].name, "main.rs");
                assert_eq!(t.entries[2].name, "script.sh");
                assert_eq!(t.entries[2].mode, TreeEntryMode::Executable);
                assert_eq!(t.entries[3].name, "src");
                assert_eq!(t.entries[3].mode, TreeEntryMode::Directory);
            }
            _ => panic!("expected tree"),
        }

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.object_type, ObjectType::Tree);

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.object_type, ObjectType::Commit);
        let obj = entry.into_object().unwrap();
        match obj {
            Object::Commit(c) => {
                assert_eq!(c.tree, tree_oid);
                assert_eq!(c.parents.len(), 1);
                assert_eq!(c.parents[0], parent_commit_oid);
                assert_eq!(c.author.name, "Author Name");
                assert_eq!(c.author.email, "author@example.com");
                assert_eq!(c.author.timestamp, 1704067200);
                assert_eq!(c.author.timezone, "+0530");
                assert_eq!(c.committer.name, "Committer Name");
                assert_eq!(c.committer.timezone, "-0800");
                assert_eq!(c.message, "Test commit message\nWith multiple lines");
            }
            _ => panic!("expected commit"),
        }

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.object_type, ObjectType::Commit);
        let obj = entry.into_object().unwrap();
        match obj {
            Object::Commit(c) => {
                assert_eq!(c.message, "Initial commit");
                assert_eq!(c.author.name, "Test");
            }
            _ => panic!("expected commit"),
        }

        let entry = reader.read_next().unwrap().unwrap();
        assert_eq!(entry.object_type, ObjectType::Tag);
        let result = entry.into_object();
        assert!(matches!(
            result,
            Err(FrontendError::InvalidObjectType { .. })
        ));

        assert_eq!(reader.objects_remaining(), 0);
        assert!(reader.read_next().unwrap().is_none());

        let all_entries = PackReader::new(pack_data).unwrap().read_all().unwrap();
        assert_eq!(all_entries.len(), 8);

        let empty_writer = PackWriter::new();
        let empty_data = empty_writer.build().unwrap();
        assert_eq!(&empty_data[0..4], PACK_SIGNATURE);
        assert_eq!(
            u32::from_be_bytes([empty_data[8], empty_data[9], empty_data[10], empty_data[11]]),
            0
        );

        let default_writer = PackWriter::default();
        assert_eq!(default_writer.entry_count(), 0);

        let raw_oid = Oid::hash(b"raw content");
        let raw_entry = PackEntry::new(
            raw_oid,
            ObjectType::Blob,
            Bytes::from_static(b"raw content"),
        );
        let mut raw_writer = PackWriter::new();
        raw_writer.add_entry(raw_entry);
        assert_eq!(raw_writer.entry_count(), 1);
    }

    #[test]
    fn test_pack_streaming_and_headerless_modes() {
        let blob = Blob::new(b"streaming content".to_vec());
        let blob_oid = blob.oid;
        let tree = Tree::new(vec![TreeEntry::file("f", Oid::hash(b"x"))]);
        let author = Signature::new("A", "a@e.com", 0, "+0000");
        let commit = Commit::new(Oid::hash(b"t"), vec![], author.clone(), author, "msg");
        let tag_entry = PackEntry::new(
            Oid::hash(b"tag"),
            ObjectType::Tag,
            Bytes::from_static(b"tag"),
        );

        let mut writer = PackWriter::new();
        writer.add_object(&Object::Blob(blob.clone())).unwrap();
        writer.add_object(&Object::Tree(tree.clone())).unwrap();
        writer.add_object(&Object::Commit(commit.clone())).unwrap();
        writer.add_entry(tag_entry);

        let mut streamed = Vec::new();
        writer.build_streaming(&mut streamed).unwrap();

        assert_eq!(&streamed[0..4], PACK_SIGNATURE);
        assert_eq!(
            u32::from_be_bytes([streamed[4], streamed[5], streamed[6], streamed[7]]),
            PACK_VERSION
        );
        assert_eq!(
            u32::from_be_bytes([streamed[8], streamed[9], streamed[10], streamed[11]]),
            4
        );

        let entries = PackReader::new(Bytes::from(streamed))
            .unwrap()
            .read_all()
            .unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].object_type, ObjectType::Blob);
        assert_eq!(entries[1].object_type, ObjectType::Tree);
        assert_eq!(entries[2].object_type, ObjectType::Commit);
        assert_eq!(entries[3].object_type, ObjectType::Tag);

        let mut headerless_writer = PackWriter::without_header();
        let blob2 = Blob::new(b"raw test".to_vec());
        let blob2_oid = blob2.oid;
        headerless_writer.add_object(&Object::Blob(blob2)).unwrap();

        let headerless_data = headerless_writer.build().unwrap();
        assert_ne!(&headerless_data[0..4], PACK_SIGNATURE);

        let mut raw_reader = PackReader::from_raw(headerless_data, 1);
        let entry = raw_reader.read_next().unwrap().unwrap();
        assert_eq!(entry.oid, blob2_oid);

        let mut headerless_streamed_writer = PackWriter::without_header();
        headerless_streamed_writer
            .add_object(&Object::Blob(blob))
            .unwrap();

        let mut headerless_output = Vec::new();
        headerless_streamed_writer
            .build_streaming(&mut headerless_output)
            .unwrap();
        assert_ne!(&headerless_output[0..4], PACK_SIGNATURE);

        let mut raw_reader2 = PackReader::from_raw(Bytes::from(headerless_output), 1);
        let entry = raw_reader2.read_next().unwrap().unwrap();
        assert_eq!(entry.oid, blob_oid);
    }

    #[test]
    fn test_pack_reader_error_handling() {
        let result = PackReader::new(Bytes::from_static(b"PACK\x00\x00"));
        assert!(result.is_err());

        let result = PackReader::new(Bytes::from_static(
            b"NOTPACK\x00\x00\x00\x02\x00\x00\x00\x00",
        ));
        assert!(result.is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(99);
        data.put_u32(0);
        let result = PackReader::new(data.freeze());
        assert!(result.is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert_eq!(reader.object_count(), 1);
        assert!(reader.read_next().is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8(0x80 | 0x30);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(reader.read_next().is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8(0x50);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(reader.read_next().is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8((OBJ_BLOB << 4) | 10);
        let compressed = compress_data(b"short").unwrap();
        data.put_slice(&compressed);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        let result = reader.read_next();
        assert!(
            matches!(result, Err(FrontendError::InvalidPackFormat(msg)) if msg.contains("decompressed size mismatch"))
        );

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8((OBJ_OFS_DELTA << 4) | 5);
        data.put_u8(0x01);
        let compressed = compress_data(b"delta").unwrap();
        data.put_slice(&compressed);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        let result = reader.read_next();
        assert!(
            matches!(result, Err(FrontendError::InvalidPackFormat(msg)) if msg.contains("offset delta not yet supported"))
        );

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8((OBJ_OFS_DELTA << 4) | 5);
        data.put_u8(0x80 | 0x01);
        data.put_u8(0x01);
        let compressed = compress_data(b"delta").unwrap();
        data.put_slice(&compressed);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(reader.read_next().is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8((OBJ_OFS_DELTA << 4) | 5);
        data.put_u8(0x80 | 0x01);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(reader.read_next().is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8(OBJ_OFS_DELTA << 4);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(reader.read_next().is_err());

        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8((OBJ_REF_DELTA << 4) | 5);
        data.put_slice(&[0u8; 16]);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(reader.read_next().is_err());

        let oid = Oid::hash(b"nonexistent");
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(10));
        delta.extend(encode_delta_size(5));
        delta.push(0x80 | 0x10);
        delta.push(5);
        let delta_compressed = compress_data(&delta).unwrap();
        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(1);
        data.put_u8((OBJ_REF_DELTA << 4) | (delta.len() as u8 & 0x0f));
        data.put_slice(oid.as_bytes());
        data.put_slice(&delta_compressed);
        let mut reader = PackReader::new(data.freeze()).unwrap();
        assert!(matches!(
            reader.read_next(),
            Err(FrontendError::ObjectNotFound(_))
        ));
    }

    #[test]
    fn test_serialization_parsing_error_handling() {
        let result = parse_tree_entries(b"100644filenamewoohoo");
        assert!(result.is_err());

        let result = parse_tree_entries(b"100644 filename");
        assert!(result.is_err());

        let mut truncated_oid = Vec::new();
        truncated_oid.extend_from_slice(b"100644 filename");
        truncated_oid.push(0);
        truncated_oid.extend_from_slice(&[0u8; 16]);
        let result = parse_tree_entries(&truncated_oid);
        assert!(result.is_err());

        let invalid_mode = [
            0xFF, 0xFE, b' ', b'n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let result = parse_tree_entries(&invalid_mode);
        assert!(result.is_err());

        let mut invalid_name = Vec::new();
        invalid_name.extend_from_slice(b"100644 ");
        invalid_name.extend_from_slice(&[0xFF, 0xFE]);
        invalid_name.push(0);
        invalid_name.extend_from_slice(&[0u8; 32]);
        let result = parse_tree_entries(&invalid_name);
        assert!(result.is_err());

        let result = parse_commit(Oid::hash(b"test"), &[0xFF, 0xFE, 0x00]);
        assert!(result.is_err());

        let result = parse_commit(Oid::hash(b"test"), b"author Test <test@test.com> 1234567890 +0000\ncommitter Test <test@test.com> 1234567890 +0000\n\nMessage");
        assert!(result.is_err());

        let tree_hex = Oid::hash(b"tree").to_string();
        let data = format!(
            "tree {}\ncommitter Test <test@test.com> 1234567890 +0000\n\nMessage",
            tree_hex
        );
        let result = parse_commit(Oid::hash(b"test"), data.as_bytes());
        assert!(result.is_err());

        let data = format!(
            "tree {}\nauthor Test <test@test.com> 1234567890 +0000\n\nMessage",
            tree_hex
        );
        let result = parse_commit(Oid::hash(b"test"), data.as_bytes());
        assert!(result.is_err());

        let result = parse_signature("Name email@example.com> 1234567890 +0000");
        assert!(result.is_err());

        let result = parse_signature("Name <email@example.com 1234567890 +0000");
        assert!(result.is_err());

        let result = parse_signature("Name <email@example.com> 1234567890");
        assert!(result.is_err());

        let result = parse_signature("Name <email@example.com> notanumber +0000");
        assert!(result.is_err());
    }

    #[test]
    fn test_delta_operations() {
        let (size, consumed) = read_delta_size(&[0x05]).unwrap();
        assert_eq!(size, 5);
        assert_eq!(consumed, 1);

        let (size, consumed) = read_delta_size(&[0x80, 0x01]).unwrap();
        assert_eq!(size, 128);
        assert_eq!(consumed, 2);

        assert!(read_delta_size(&[]).is_err());

        let base = b"hello world";
        let delta = vec![11, 5, 0x80 | 0x01 | 0x10, 0, 5];
        let result = apply_delta_instructions(base, &delta).unwrap();
        assert_eq!(result, b"hello");

        let base = b"hello";
        let mut delta = vec![5, 11, 0x80 | 0x01 | 0x10, 0, 5, 6];
        delta.extend_from_slice(b" world");
        let result = apply_delta_instructions(base, &delta).unwrap();
        assert_eq!(result, b"hello world");

        let base = vec![b'A'; 512];
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(512));
        delta.extend(encode_delta_size(5));
        delta.push(0x80 | 0x02 | 0x10);
        delta.push(0x01);
        delta.push(5);
        let result = apply_delta_instructions(&base, &delta).unwrap();
        assert_eq!(result.len(), 5);

        let base = vec![b'B'; 0x20000];
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(0x20000));
        delta.extend(encode_delta_size(5));
        delta.push(0x80 | 0x04 | 0x10);
        delta.push(0x01);
        delta.push(5);
        let result = apply_delta_instructions(&base, &delta).unwrap();
        assert_eq!(result.len(), 5);

        let base = vec![b'C'; 256];
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(256));
        delta.extend(encode_delta_size(5));
        delta.push(0x80 | 0x08 | 0x10);
        delta.push(0x00);
        delta.push(5);
        let result = apply_delta_instructions(&base, &delta).unwrap();
        assert_eq!(result.len(), 5);

        let base = vec![b'D'; 512];
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(512));
        delta.extend(encode_delta_size(0x102));
        delta.push(0x80 | 0x10 | 0x20);
        delta.push(0x02);
        delta.push(0x01);
        let result = apply_delta_instructions(&base, &delta).unwrap();
        assert_eq!(result.len(), 0x102);

        let base = vec![b'E'; 0x20000];
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(0x20000));
        delta.extend(encode_delta_size(0x10002));
        delta.push(0x80 | 0x10 | 0x40);
        delta.push(0x02);
        delta.push(0x01);
        let result = apply_delta_instructions(&base, &delta).unwrap();
        assert_eq!(result.len(), 0x10002);

        let base = vec![b'F'; 0x10000];
        let mut delta = Vec::new();
        delta.extend(encode_delta_size(0x10000));
        delta.extend(encode_delta_size(0x10000));
        delta.push(0x80);
        let result = apply_delta_instructions(&base, &delta).unwrap();
        assert_eq!(result.len(), 0x10000);

        let base = b"small";
        let delta = vec![5, 100, 0x80 | 0x01 | 0x10, 0, 100];
        assert!(apply_delta_instructions(base, &delta).is_err());

        let base = b"base";
        let mut delta = vec![4, 10, 10];
        delta.extend_from_slice(b"short");
        assert!(apply_delta_instructions(base, &delta).is_err());

        let base = b"base";
        let delta = vec![4, 4, 0];
        assert!(apply_delta_instructions(base, &delta).is_err());

        let base = b"hello";
        let delta = vec![5, 10, 0x80 | 0x01 | 0x10, 0, 5];
        assert!(apply_delta_instructions(base, &delta).is_err());
    }

    #[test]
    fn test_ref_delta_roundtrip() {
        let mut writer = PackWriter::new();
        let blob = Blob::new(b"base content for delta".to_vec());
        let base_oid = blob.oid;
        writer.add_object(&Object::Blob(blob)).unwrap();
        let pack_data = writer.build().unwrap();

        let mut reader = PackReader::new(pack_data).unwrap();
        let base_entry = reader.read_next().unwrap().unwrap();
        assert_eq!(base_entry.oid, base_oid);

        let mut delta = Vec::new();
        delta.extend(encode_delta_size(22));
        delta.extend(encode_delta_size(11));
        delta.push(0x80 | 0x10);
        delta.push(11);

        let delta_compressed = compress_data(&delta).unwrap();
        let mut data = BytesMut::new();
        data.put_slice(PACK_SIGNATURE);
        data.put_u32(PACK_VERSION);
        data.put_u32(2);

        let base_blob = Blob::new(b"base content for delta".to_vec());
        let base_entry_new = PackEntry::from_object(&Object::Blob(base_blob)).unwrap();
        let mut base_writer = PackWriter::without_header();
        base_writer.add_entry(base_entry_new);
        let base_pack = base_writer.build().unwrap();
        data.put_slice(&base_pack);

        data.put_u8((OBJ_REF_DELTA << 4) | (delta.len() as u8 & 0x0f));
        data.put_slice(base_oid.as_bytes());
        data.put_slice(&delta_compressed);

        let mut reader2 = PackReader::new(data.freeze()).unwrap();
        let first = reader2.read_next().unwrap().unwrap();
        assert_eq!(first.object_type, ObjectType::Blob);

        let second = reader2.read_next().unwrap();
        assert!(second.is_some());
        let delta_entry = second.unwrap();
        assert_eq!(delta_entry.object_type, ObjectType::Blob);
        assert_eq!(delta_entry.data.len(), 11);
    }
}
