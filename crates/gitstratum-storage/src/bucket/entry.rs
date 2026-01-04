use gitstratum_core::Oid;

use crate::error::{BucketStoreError, Result};

pub const ENTRY_SIZE: usize = 32;
pub const OID_SIZE: usize = 32;
pub const OID_PREFIX_SIZE: usize = 16;
pub const OID_SUFFIX_SIZE: usize = 16;

pub const MAX_OFFSET: u64 = 0x0000_FFFF_FFFF_FFFF;
pub const MAX_SIZE: u32 = 0x00FF_FFFF;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct CompactEntry {
    pub oid_suffix: [u8; OID_SUFFIX_SIZE],
    pub file_id: u16,
    pub offset: [u8; 6],
    pub size: [u8; 3],
    pub flags: u8,
    pub _reserved: [u8; 4],
}

// Compile-time assertions to ensure transmute safety
const _: () = assert!(std::mem::size_of::<CompactEntry>() == ENTRY_SIZE);
const _: () = assert!(std::mem::align_of::<CompactEntry>() == 1); // packed = no padding

impl CompactEntry {
    pub const EMPTY: Self = Self {
        oid_suffix: [0u8; OID_SUFFIX_SIZE],
        file_id: 0,
        offset: [0u8; 6],
        size: [0u8; 3],
        flags: 0,
        _reserved: [0u8; 4],
    };

    pub fn new(oid: &Oid, file_id: u16, offset: u64, size: u32, flags: u8) -> Result<Self> {
        if offset > MAX_OFFSET {
            return Err(BucketStoreError::EntryOffsetTooLarge {
                offset,
                max: MAX_OFFSET,
            });
        }
        if size > MAX_SIZE {
            return Err(BucketStoreError::EntrySizeTooLarge {
                size,
                max: MAX_SIZE,
            });
        }

        let mut oid_suffix = [0u8; OID_SUFFIX_SIZE];
        oid_suffix.copy_from_slice(&oid.as_bytes()[OID_PREFIX_SIZE..OID_SIZE]);

        let mut offset_bytes = [0u8; 6];
        offset_bytes.copy_from_slice(&offset.to_be_bytes()[2..8]);

        let mut size_bytes = [0u8; 3];
        size_bytes.copy_from_slice(&size.to_be_bytes()[1..4]);

        Ok(Self {
            oid_suffix,
            file_id,
            offset: offset_bytes,
            size: size_bytes,
            flags,
            _reserved: [0u8; 4],
        })
    }

    pub fn is_empty(&self) -> bool {
        self.oid_suffix == [0u8; OID_SUFFIX_SIZE]
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & EntryFlags::DELETED.bits() != 0
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & EntryFlags::COMPRESSED.bits() != 0
    }

    pub fn matches(&self, oid: &Oid) -> bool {
        self.oid_suffix == oid.as_bytes()[OID_PREFIX_SIZE..OID_SIZE]
    }

    /// Reconstruct full OID from prefix (derived from bucket_id) and stored suffix.
    pub fn reconstruct_oid(&self, oid_prefix: &[u8; OID_PREFIX_SIZE]) -> Oid {
        let mut bytes = [0u8; OID_SIZE];
        bytes[..OID_PREFIX_SIZE].copy_from_slice(oid_prefix);
        bytes[OID_PREFIX_SIZE..].copy_from_slice(&self.oid_suffix);
        Oid::from_bytes(bytes)
    }

    pub fn offset(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf[2..8].copy_from_slice(&self.offset);
        u64::from_be_bytes(buf)
    }

    pub fn size(&self) -> u32 {
        let mut buf = [0u8; 4];
        buf[1..4].copy_from_slice(&self.size);
        u32::from_be_bytes(buf)
    }

    /// Convert to raw bytes for disk storage.
    ///
    /// # Safety (internal)
    /// Safe because:
    /// - `#[repr(C, packed)]` guarantees deterministic layout with no padding
    /// - Compile-time assertions verify size == ENTRY_SIZE and alignment == 1
    /// - All fields are plain data (no pointers, no Drop)
    #[allow(clippy::wrong_self_convention)]
    pub fn to_bytes(&self) -> [u8; ENTRY_SIZE] {
        // SAFETY: CompactEntry is repr(C, packed) with compile-time size/alignment checks
        unsafe { std::mem::transmute_copy(self) }
    }

    /// Restore from raw bytes read from disk.
    ///
    /// # Safety (internal)
    /// Safe because:
    /// - Input is exactly ENTRY_SIZE bytes (enforced by type)
    /// - `#[repr(C, packed)]` guarantees any bit pattern is valid
    /// - No invalid states possible (all fields are integers/byte arrays)
    pub fn from_bytes(bytes: &[u8; ENTRY_SIZE]) -> Self {
        // SAFETY: CompactEntry is repr(C, packed), all bit patterns valid
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct EntryFlags: u8 {
        const NONE = 0;
        const COMPRESSED = 1 << 0;
        const DELETED = 1 << 1;
        const LARGE_OBJECT = 1 << 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_oid(seed: u8) -> Oid {
        let mut bytes = [seed; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        Oid::from_bytes(bytes)
    }

    #[test]
    fn test_entry_size() {
        assert_eq!(std::mem::size_of::<CompactEntry>(), ENTRY_SIZE);
    }

    #[test]
    fn test_entry_empty() {
        assert!(CompactEntry::EMPTY.is_empty());
    }

    #[test]
    fn test_entry_new() {
        let oid = create_test_oid(0x42);
        let entry = CompactEntry::new(&oid, 10, 1000, 2048, EntryFlags::NONE.bits()).unwrap();

        assert!(!entry.is_empty());
        assert!(entry.matches(&oid));
        let file_id = { entry.file_id };
        assert_eq!(file_id, 10);
        assert_eq!(entry.offset(), 1000);
        assert_eq!(entry.size(), 2048);
        let flags = { entry.flags };
        assert_eq!(flags, 0);
    }

    #[test]
    fn test_entry_large_offset() {
        let oid = create_test_oid(0x01);
        let offset = MAX_OFFSET;
        let entry = CompactEntry::new(&oid, 1, offset, 100, 0).unwrap();
        assert_eq!(entry.offset(), offset);
    }

    #[test]
    fn test_entry_offset_too_large() {
        let oid = create_test_oid(0x01);
        let offset = MAX_OFFSET + 1;
        let result = CompactEntry::new(&oid, 1, offset, 100, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_large_size() {
        let oid = create_test_oid(0x01);
        let size = MAX_SIZE;
        let entry = CompactEntry::new(&oid, 1, 0, size, 0).unwrap();
        assert_eq!(entry.size(), size);
    }

    #[test]
    fn test_entry_size_too_large() {
        let oid = create_test_oid(0x01);
        let size = MAX_SIZE + 1;
        let result = CompactEntry::new(&oid, 1, 0, size, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_serialization() {
        let oid = create_test_oid(0x42);
        let entry = CompactEntry::new(&oid, 10, 1000, 2048, EntryFlags::COMPRESSED.bits()).unwrap();

        let bytes = entry.to_bytes();
        assert_eq!(bytes.len(), ENTRY_SIZE);

        let restored = CompactEntry::from_bytes(&bytes);
        assert_eq!(entry.oid_suffix, restored.oid_suffix);
        let entry_file_id = { entry.file_id };
        let restored_file_id = { restored.file_id };
        assert_eq!(entry_file_id, restored_file_id);
        assert_eq!(entry.offset(), restored.offset());
        assert_eq!(entry.size(), restored.size());
        let entry_flags = { entry.flags };
        let restored_flags = { restored.flags };
        assert_eq!(entry_flags, restored_flags);
    }

    #[test]
    fn test_entry_matches() {
        let oid1 = create_test_oid(0x01);
        let oid2 = create_test_oid(0x02);
        let entry = CompactEntry::new(&oid1, 1, 0, 100, 0).unwrap();

        assert!(entry.matches(&oid1));
        assert!(!entry.matches(&oid2));
    }

    #[test]
    fn test_entry_flags() {
        assert_eq!(EntryFlags::NONE.bits(), 0);
        assert_eq!(EntryFlags::COMPRESSED.bits(), 1);
        assert_eq!(EntryFlags::DELETED.bits(), 2);
        assert_eq!(EntryFlags::LARGE_OBJECT.bits(), 4);
    }

    #[test]
    fn test_is_deleted() {
        let oid = create_test_oid(0x01);
        let entry = CompactEntry::new(&oid, 1, 0, 100, EntryFlags::DELETED.bits()).unwrap();
        assert!(entry.is_deleted());

        let entry2 = CompactEntry::new(&oid, 1, 0, 100, EntryFlags::NONE.bits()).unwrap();
        assert!(!entry2.is_deleted());

        let entry3 = CompactEntry::new(
            &oid,
            1,
            0,
            100,
            EntryFlags::DELETED.bits() | EntryFlags::COMPRESSED.bits(),
        )
        .unwrap();
        assert!(entry3.is_deleted());
    }

    #[test]
    fn test_is_compressed() {
        let oid = create_test_oid(0x01);
        let entry = CompactEntry::new(&oid, 1, 0, 100, EntryFlags::COMPRESSED.bits()).unwrap();
        assert!(entry.is_compressed());

        let entry2 = CompactEntry::new(&oid, 1, 0, 100, EntryFlags::NONE.bits()).unwrap();
        assert!(!entry2.is_compressed());
    }

    #[test]
    fn test_reconstruct_oid() {
        let oid = create_test_oid(0x42);
        let entry = CompactEntry::new(&oid, 1, 0, 100, 0).unwrap();

        let prefix: [u8; OID_PREFIX_SIZE] = oid.as_bytes()[..OID_PREFIX_SIZE].try_into().unwrap();
        let reconstructed = entry.reconstruct_oid(&prefix);

        assert_eq!(reconstructed, oid);
    }

    #[test]
    fn test_constants() {
        assert_eq!(OID_SIZE, 32);
        assert_eq!(OID_PREFIX_SIZE, 16);
        assert_eq!(OID_SUFFIX_SIZE, 16);
        assert_eq!(OID_PREFIX_SIZE + OID_SUFFIX_SIZE, OID_SIZE);
    }
}
