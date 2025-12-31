use gitstratum_core::Oid;

pub const ENTRY_SIZE: usize = 32;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct CompactEntry {
    pub oid_suffix: [u8; 16],
    pub file_id: u16,
    pub offset: [u8; 6],
    pub size: [u8; 3],
    pub flags: u8,
    pub _reserved: [u8; 4],
}

impl CompactEntry {
    pub const EMPTY: Self = Self {
        oid_suffix: [0u8; 16],
        file_id: 0,
        offset: [0u8; 6],
        size: [0u8; 3],
        flags: 0,
        _reserved: [0u8; 4],
    };

    pub fn new(oid: &Oid, file_id: u16, offset: u64, size: u32, flags: u8) -> Self {
        let mut oid_suffix = [0u8; 16];
        oid_suffix.copy_from_slice(&oid.as_bytes()[16..32]);

        let mut offset_bytes = [0u8; 6];
        offset_bytes.copy_from_slice(&offset.to_be_bytes()[2..8]);

        let mut size_bytes = [0u8; 3];
        size_bytes.copy_from_slice(&size.to_be_bytes()[1..4]);

        Self {
            oid_suffix,
            file_id,
            offset: offset_bytes,
            size: size_bytes,
            flags,
            _reserved: [0u8; 4],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.oid_suffix == [0u8; 16]
    }

    pub fn matches(&self, oid: &Oid) -> bool {
        self.oid_suffix == oid.as_bytes()[16..32]
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

    pub fn to_bytes(&self) -> [u8; ENTRY_SIZE] {
        unsafe { std::mem::transmute_copy(self) }
    }

    pub fn from_bytes(bytes: &[u8; ENTRY_SIZE]) -> Self {
        unsafe { std::ptr::read(bytes.as_ptr() as *const Self) }
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
        let entry = CompactEntry::new(&oid, 10, 1000, 2048, EntryFlags::NONE.bits());

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
        let offset = 0x0000_FFFF_FFFF_FFFF;
        let entry = CompactEntry::new(&oid, 1, offset, 100, 0);
        assert_eq!(entry.offset(), offset);
    }

    #[test]
    fn test_entry_large_size() {
        let oid = create_test_oid(0x01);
        let size = 0x00FF_FFFF;
        let entry = CompactEntry::new(&oid, 1, 0, size, 0);
        assert_eq!(entry.size(), size);
    }

    #[test]
    fn test_entry_serialization() {
        let oid = create_test_oid(0x42);
        let entry = CompactEntry::new(&oid, 10, 1000, 2048, EntryFlags::COMPRESSED.bits());

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
        let entry = CompactEntry::new(&oid1, 1, 0, 100, 0);

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
}
