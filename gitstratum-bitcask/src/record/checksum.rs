use super::header::RecordHeader;
use crc32fast::Hasher;

pub fn compute_crc32(header: &RecordHeader, key: &[u8], value: &[u8]) -> u32 {
    let mut hasher = Hasher::new();

    hasher.update(&header.magic.to_le_bytes());
    hasher.update(&[0u8; 4]); // CRC placeholder
    hasher.update(&header.flags.to_le_bytes());
    hasher.update(&header.key_len.to_le_bytes());
    hasher.update(&header.value_len.to_le_bytes());
    hasher.update(&header.timestamp.to_le_bytes());
    hasher.update(&header.reserved.to_le_bytes());
    hasher.update(key);
    hasher.update(value);

    hasher.finalize()
}

pub fn verify_crc32(header: &RecordHeader, key: &[u8], value: &[u8]) -> bool {
    let computed = compute_crc32(header, key, value);
    computed == header.crc32
}
