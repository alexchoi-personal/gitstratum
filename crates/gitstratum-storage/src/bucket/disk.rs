use super::entry::{CompactEntry, ENTRY_SIZE};
use crate::error::{BucketStoreError, Result};

pub const BUCKET_SIZE: usize = 4096;
pub const BUCKET_MAGIC: u32 = 0x424B4854; // "BKHT"
pub const HEADER_SIZE: usize = 32;
pub const MAX_ENTRIES: usize = 126;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct BucketHeader {
    pub magic: u32,
    pub count: u16,
    pub crc32: u32,
    pub _reserved: [u8; 22],
}

impl BucketHeader {
    pub fn new(count: u16) -> Self {
        Self {
            magic: BUCKET_MAGIC,
            count,
            crc32: 0,
            _reserved: [0u8; 22],
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.count.to_le_bytes());
        buf[6..10].copy_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes(bytes[0..4].try_into().expect("slice is exactly 4 bytes")),
            count: u16::from_le_bytes(bytes[4..6].try_into().expect("slice is exactly 2 bytes")),
            crc32: u32::from_le_bytes(bytes[6..10].try_into().expect("slice is exactly 4 bytes")),
            _reserved: [0u8; 22],
        }
    }
}

#[repr(C, packed)]
#[derive(Clone)]
pub struct DiskBucket {
    pub header: BucketHeader,
    pub entries: [CompactEntry; MAX_ENTRIES],
    pub next_overflow: u32,
    pub _padding: [u8; 28],
}

impl DiskBucket {
    pub fn new() -> Self {
        Self {
            header: BucketHeader::new(0),
            entries: [CompactEntry::EMPTY; MAX_ENTRIES],
            next_overflow: 0,
            _padding: [0u8; 28],
        }
    }

    pub fn from_bytes(bytes: &[u8; BUCKET_SIZE]) -> Result<Self> {
        let header = BucketHeader::from_bytes(
            bytes[0..HEADER_SIZE]
                .try_into()
                .expect("slice is exactly HEADER_SIZE bytes"),
        );

        if { header.magic } == 0 {
            return Ok(Self::new());
        }

        if { header.magic } != BUCKET_MAGIC {
            return Err(BucketStoreError::InvalidMagic {
                expected: BUCKET_MAGIC,
                actual: { header.magic },
            });
        }

        let mut entries = [CompactEntry::EMPTY; MAX_ENTRIES];
        for (i, entry) in entries.iter_mut().enumerate() {
            let start = HEADER_SIZE + i * ENTRY_SIZE;
            let end = start + ENTRY_SIZE;
            *entry = CompactEntry::from_bytes(
                bytes[start..end]
                    .try_into()
                    .expect("slice is exactly ENTRY_SIZE bytes"),
            );
        }

        let next_offset = HEADER_SIZE + MAX_ENTRIES * ENTRY_SIZE;
        let next_overflow = u32::from_le_bytes(
            bytes[next_offset..next_offset + 4]
                .try_into()
                .expect("slice is exactly 4 bytes"),
        );

        Ok(Self {
            header,
            entries,
            next_overflow,
            _padding: [0u8; 28],
        })
    }

    pub fn to_bytes(&self) -> [u8; BUCKET_SIZE] {
        let mut buf = [0u8; BUCKET_SIZE];
        buf[0..HEADER_SIZE].copy_from_slice(&self.header.to_bytes());

        for i in 0..MAX_ENTRIES {
            let start = HEADER_SIZE + i * ENTRY_SIZE;
            buf[start..start + ENTRY_SIZE].copy_from_slice(&self.entries[i].to_bytes());
        }

        let next_offset = HEADER_SIZE + MAX_ENTRIES * ENTRY_SIZE;
        buf[next_offset..next_offset + 4].copy_from_slice(&self.next_overflow.to_le_bytes());

        buf
    }

    pub fn find_entry(&self, oid: &gitstratum_core::Oid) -> Option<&CompactEntry> {
        for i in 0..self.header.count as usize {
            if self.entries[i].matches(oid) {
                return Some(&self.entries[i]);
            }
        }
        None
    }

    pub fn insert(&mut self, entry: CompactEntry) -> Result<()> {
        if self.header.count as usize >= MAX_ENTRIES {
            return Err(BucketStoreError::BucketOverflow { bucket_id: 0 });
        }
        self.entries[self.header.count as usize] = entry;
        self.header.count += 1;
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.header.count as usize >= MAX_ENTRIES
    }
}

impl Default for DiskBucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Oid;

    fn create_test_oid(seed: u8) -> Oid {
        let mut bytes = [seed; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        Oid::from_bytes(bytes)
    }

    #[test]
    fn test_bucket_size() {
        assert_eq!(std::mem::size_of::<DiskBucket>(), BUCKET_SIZE);
    }

    #[test]
    fn test_bucket_new() {
        let bucket = DiskBucket::new();
        let magic = { bucket.header.magic };
        let count = { bucket.header.count };
        assert_eq!(magic, BUCKET_MAGIC);
        assert_eq!(count, 0);
        assert!(!bucket.is_full());
    }

    #[test]
    fn test_bucket_insert() {
        let mut bucket = DiskBucket::new();
        let oid = create_test_oid(0x01);
        let entry = CompactEntry::new(&oid, 1, 0, 100, 0).unwrap();

        bucket.insert(entry).unwrap();
        let count = { bucket.header.count };
        assert_eq!(count, 1);

        let found = bucket.find_entry(&oid);
        assert!(found.is_some());
        assert!(found.unwrap().matches(&oid));
    }

    #[test]
    fn test_bucket_insert_multiple() {
        let mut bucket = DiskBucket::new();

        for i in 0..10 {
            let oid = create_test_oid(i);
            let entry = CompactEntry::new(&oid, i as u16, 0, 100, 0).unwrap();
            bucket.insert(entry).unwrap();
        }

        let count = { bucket.header.count };
        assert_eq!(count, 10);

        for i in 0..10 {
            let oid = create_test_oid(i);
            assert!(bucket.find_entry(&oid).is_some());
        }
    }

    #[test]
    fn test_bucket_serialization() {
        let mut bucket = DiskBucket::new();

        for i in 0..5 {
            let oid = create_test_oid(i);
            let entry = CompactEntry::new(&oid, i as u16, i as u64 * 1000, (i as u32 + 1) * 100, 0)
                .unwrap();
            bucket.insert(entry).unwrap();
        }

        let bytes = bucket.to_bytes();
        assert_eq!(bytes.len(), BUCKET_SIZE);

        let restored = DiskBucket::from_bytes(&bytes).unwrap();
        let magic = { restored.header.magic };
        let count = { restored.header.count };
        assert_eq!(magic, BUCKET_MAGIC);
        assert_eq!(count, 5);

        for i in 0..5 {
            let oid = create_test_oid(i);
            assert!(restored.find_entry(&oid).is_some());
        }
    }

    #[test]
    fn test_bucket_overflow() {
        let mut bucket = DiskBucket::new();

        for i in 0..MAX_ENTRIES {
            let oid = create_test_oid(i as u8);
            let entry = CompactEntry::new(&oid, 1, 0, 100, 0).unwrap();
            bucket.insert(entry).unwrap();
        }

        assert!(bucket.is_full());

        let oid = create_test_oid(0xFF);
        let entry = CompactEntry::new(&oid, 1, 0, 100, 0).unwrap();
        assert!(bucket.insert(entry).is_err());
    }

    #[test]
    fn test_bucket_find_nonexistent() {
        let bucket = DiskBucket::new();
        let oid = create_test_oid(0x01);
        assert!(bucket.find_entry(&oid).is_none());
    }

    #[test]
    fn test_bucket_header_serialization() {
        let header = BucketHeader::new(42);
        let bytes = header.to_bytes();
        let restored = BucketHeader::from_bytes(&bytes);

        let magic = { restored.magic };
        let count = { restored.count };
        assert_eq!(magic, BUCKET_MAGIC);
        assert_eq!(count, 42);
    }
}
