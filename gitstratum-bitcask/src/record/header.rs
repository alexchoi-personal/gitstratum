use crate::bucket::EntryFlags;

pub const HEADER_SIZE: usize = 40;
pub const KEY_SIZE: usize = 32;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub magic: u32,
    pub crc32: u32,
    pub flags: u32,
    pub key_len: u32,
    pub value_len: u64,
    pub timestamp: u64,
    pub reserved: u64,
}

pub const RECORD_HEADER_MAGIC: u32 = 0x42435356; // "BCSV"

impl RecordHeader {
    pub fn new(value_len: u64, timestamp: u64, flags: EntryFlags) -> Self {
        Self {
            magic: RECORD_HEADER_MAGIC,
            crc32: 0,
            flags: flags.bits() as u32,
            key_len: KEY_SIZE as u32,
            value_len,
            timestamp,
            reserved: 0,
        }
    }

    pub fn record_size(&self) -> usize {
        let data_size = HEADER_SIZE + KEY_SIZE + self.value_len as usize;
        align_to_block(data_size)
    }

    pub fn padding_size(&self) -> usize {
        self.record_size() - (HEADER_SIZE + KEY_SIZE + self.value_len as usize)
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.crc32.to_le_bytes());
        buf[8..12].copy_from_slice(&self.flags.to_le_bytes());
        buf[12..16].copy_from_slice(&self.key_len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.value_len.to_le_bytes());
        buf[24..32].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[32..40].copy_from_slice(&self.reserved.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != RECORD_HEADER_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            crc32: u32::from_le_bytes(buf[4..8].try_into().ok()?),
            flags: u32::from_le_bytes(buf[8..12].try_into().ok()?),
            key_len: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            value_len: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            timestamp: u64::from_le_bytes(buf[24..32].try_into().ok()?),
            reserved: u64::from_le_bytes(buf[32..40].try_into().ok()?),
        })
    }

    pub fn set_crc32(&mut self, crc: u32) {
        self.crc32 = crc;
    }
}

pub const BLOCK_SIZE: usize = 4096;

pub fn align_to_block(size: usize) -> usize {
    (size + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<RecordHeader>(), HEADER_SIZE);
    }

    #[test]
    fn test_header_new() {
        let header = RecordHeader::new(1000, 12345, EntryFlags::NONE);
        assert_eq!(header.magic, RECORD_HEADER_MAGIC);
        assert_eq!(header.value_len, 1000);
        assert_eq!(header.timestamp, 12345);
        assert_eq!(header.key_len, KEY_SIZE as u32);
    }

    #[test]
    fn test_header_serialization() {
        let header = RecordHeader::new(1000, 12345, EntryFlags::COMPRESSED);

        let bytes = header.to_bytes();
        let restored = RecordHeader::from_bytes(&bytes).unwrap();

        assert_eq!(restored.magic, RECORD_HEADER_MAGIC);
        assert_eq!(restored.value_len, 1000);
        assert_eq!(restored.timestamp, 12345);
        assert_eq!(restored.flags, EntryFlags::COMPRESSED.bits() as u32);
    }

    #[test]
    fn test_header_from_bytes_invalid_magic() {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        assert!(RecordHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_header_from_bytes_too_short() {
        let bytes = [0u8; HEADER_SIZE - 1];
        assert!(RecordHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_record_size() {
        let header = RecordHeader::new(100, 0, EntryFlags::NONE);
        let size = header.record_size();
        assert_eq!(size % BLOCK_SIZE, 0);
        assert!(size >= HEADER_SIZE + KEY_SIZE + 100);
    }

    #[test]
    fn test_padding_size() {
        let header = RecordHeader::new(100, 0, EntryFlags::NONE);
        let padding = header.padding_size();
        let total = HEADER_SIZE + KEY_SIZE + 100 + padding;
        assert_eq!(total % BLOCK_SIZE, 0);
    }

    #[test]
    fn test_set_crc32() {
        let mut header = RecordHeader::new(100, 0, EntryFlags::NONE);
        assert_eq!(header.crc32, 0);

        header.set_crc32(0xDEADBEEF);
        assert_eq!(header.crc32, 0xDEADBEEF);
    }

    #[test]
    fn test_align_to_block() {
        assert_eq!(align_to_block(0), 0);
        assert_eq!(align_to_block(1), 4096);
        assert_eq!(align_to_block(4096), 4096);
        assert_eq!(align_to_block(4097), 8192);
        assert_eq!(align_to_block(8192), 8192);
    }
}
