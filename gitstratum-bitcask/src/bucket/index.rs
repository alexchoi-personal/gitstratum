use gitstratum_core::Oid;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct BucketIndex {
    offsets: Vec<u32>,
    bucket_count: u32,
    entry_count: AtomicU64,
}

impl BucketIndex {
    pub fn new(bucket_count: u32) -> Self {
        let mut offsets = Vec::with_capacity(bucket_count as usize);
        for i in 0..bucket_count {
            offsets.push(i);
        }
        Self {
            offsets,
            bucket_count,
            entry_count: AtomicU64::new(0),
        }
    }

    pub fn bucket_id(&self, oid: &Oid) -> u32 {
        let bytes = oid.as_bytes();
        let prefix = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        prefix % self.bucket_count
    }

    pub fn bucket_offset(&self, oid: &Oid) -> u64 {
        let id = self.bucket_id(oid);
        (self.offsets[id as usize] as u64) * 4096
    }

    pub fn bucket_offset_by_id(&self, bucket_id: u32) -> u64 {
        (self.offsets[bucket_id as usize] as u64) * 4096
    }

    pub fn set_bucket_offset(&mut self, bucket_id: u32, block_num: u32) {
        self.offsets[bucket_id as usize] = block_num;
    }

    pub fn bucket_count(&self) -> u32 {
        self.bucket_count
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    pub fn increment_entry_count(&self) {
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_entry_count(&self) {
        self.entry_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn memory_usage(&self) -> usize {
        self.offsets.len() * std::mem::size_of::<u32>()
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
    fn test_bucket_index_new() {
        let index = BucketIndex::new(1024);
        assert_eq!(index.bucket_count(), 1024);
        assert_eq!(index.entry_count(), 0);
    }

    #[test]
    fn test_bucket_id_distribution() {
        let index = BucketIndex::new(1024);

        let oid1 = create_test_oid(0x00);
        let oid2 = create_test_oid(0x01);

        let id1 = index.bucket_id(&oid1);
        let id2 = index.bucket_id(&oid2);

        assert!(id1 < 1024);
        assert!(id2 < 1024);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_bucket_offset() {
        let index = BucketIndex::new(1024);
        let oid = create_test_oid(0x01);

        let offset = index.bucket_offset(&oid);
        assert_eq!(offset % 4096, 0);
    }

    #[test]
    fn test_entry_count() {
        let index = BucketIndex::new(1024);
        assert_eq!(index.entry_count(), 0);

        index.increment_entry_count();
        assert_eq!(index.entry_count(), 1);

        index.increment_entry_count();
        assert_eq!(index.entry_count(), 2);

        index.decrement_entry_count();
        assert_eq!(index.entry_count(), 1);
    }

    #[test]
    fn test_set_bucket_offset() {
        let mut index = BucketIndex::new(1024);
        index.set_bucket_offset(0, 100);
        assert_eq!(index.bucket_offset_by_id(0), 100 * 4096);
    }

    #[test]
    fn test_memory_usage() {
        let index = BucketIndex::new(1024);
        assert_eq!(index.memory_usage(), 1024 * 4);
    }
}
