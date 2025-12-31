use super::checksum::compute_crc32;
use super::header::{RecordHeader, HEADER_SIZE, KEY_SIZE, RECORD_HEADER_MAGIC};
use crate::bucket::EntryFlags;
use crate::error::{BitcaskError, Result};
use bytes::Bytes;
use gitstratum_core::Oid;

pub const RECORD_MAGIC: u32 = RECORD_HEADER_MAGIC;
pub const BLOCK_SIZE: usize = 4096;
pub const MAX_OBJECT_SIZE: usize = 16 * 1024 * 1024; // 16MB

pub struct DataRecord {
    pub header: RecordHeader,
    pub oid: Oid,
    pub value: Bytes,
}

impl DataRecord {
    pub fn new(oid: Oid, value: Bytes, timestamp: u64) -> Result<Self> {
        if value.len() > MAX_OBJECT_SIZE {
            return Err(BitcaskError::ObjectTooLarge { size: value.len() });
        }

        let flags = if value.len() > 65536 {
            EntryFlags::LARGE_OBJECT
        } else {
            EntryFlags::NONE
        };

        let header = RecordHeader::new(value.len() as u64, timestamp, flags);

        Ok(Self { header, oid, value })
    }

    pub fn record_size(&self) -> usize {
        self.header.record_size()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let record_size = self.record_size();
        let mut buf = vec![0u8; record_size];

        let crc = compute_crc32(&self.header, self.oid.as_bytes(), &self.value);
        let mut header = self.header;
        header.set_crc32(crc);

        buf[0..HEADER_SIZE].copy_from_slice(&header.to_bytes());
        buf[HEADER_SIZE..HEADER_SIZE + KEY_SIZE].copy_from_slice(self.oid.as_bytes());
        buf[HEADER_SIZE + KEY_SIZE..HEADER_SIZE + KEY_SIZE + self.value.len()]
            .copy_from_slice(&self.value);

        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(BitcaskError::CorruptedRecord {
                file_id: 0,
                offset: 0,
                reason: "buffer too small for header".to_string(),
            });
        }

        let header = RecordHeader::from_bytes(buf).ok_or_else(|| BitcaskError::InvalidMagic {
            expected: RECORD_MAGIC,
            actual: u32::from_le_bytes(buf[0..4].try_into().unwrap_or([0; 4])),
        })?;

        let key_start = HEADER_SIZE;
        let key_end = key_start + KEY_SIZE;
        let value_start = key_end;
        let value_end = value_start + header.value_len as usize;

        if buf.len() < value_end {
            return Err(BitcaskError::CorruptedRecord {
                file_id: 0,
                offset: 0,
                reason: "buffer too small for value".to_string(),
            });
        }

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&buf[key_start..key_end]);
        let oid = Oid::from_bytes(key_bytes);

        let value = Bytes::copy_from_slice(&buf[value_start..value_end]);

        let computed_crc = compute_crc32(&header, &key_bytes, &value);
        if computed_crc != header.crc32 {
            return Err(BitcaskError::CrcMismatch {
                expected: header.crc32,
                actual: computed_crc,
            });
        }

        Ok(Self { header, oid, value })
    }
}
