use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use gitstratum_core::{Blob, Oid};
use parking_lot::RwLock;

#[derive(Clone)]
struct BufferEntry {
    blob: Blob,
    timestamp: Instant,
}

pub struct WriteBufferConfig {
    pub max_entries: usize,
    pub max_size_bytes: usize,
    pub max_age: Duration,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            max_size_bytes: 64 * 1024 * 1024,
            max_age: Duration::from_secs(30),
        }
    }
}

struct BufferInner {
    entries: VecDeque<BufferEntry>,
    index: HashMap<Oid, usize>,
}

impl BufferInner {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            index: HashMap::new(),
        }
    }

    fn rebuild_index(&mut self) {
        self.index.clear();
        for (i, entry) in self.entries.iter().enumerate() {
            self.index.insert(entry.blob.oid, i);
        }
    }
}

pub struct WriteBuffer {
    config: WriteBufferConfig,
    inner: RwLock<BufferInner>,
    current_size: AtomicU64,
}

impl WriteBuffer {
    pub fn new(config: WriteBufferConfig) -> Self {
        Self {
            config,
            inner: RwLock::new(BufferInner::new()),
            current_size: AtomicU64::new(0),
        }
    }

    pub fn add(&self, blob: Blob) {
        let size = blob.data.len();
        let oid = blob.oid;

        self.evict_old_entries();
        self.evict_if_needed(size);

        let entry = BufferEntry {
            blob,
            timestamp: Instant::now(),
        };

        let mut inner = self.inner.write();

        if inner.index.contains_key(&oid) {
            return;
        }

        let new_index = inner.entries.len();
        inner.entries.push_back(entry);
        inner.index.insert(oid, new_index);
        self.current_size.fetch_add(size as u64, Ordering::Relaxed);
    }

    pub fn get(&self, oid: &Oid) -> Option<Blob> {
        let inner = self.inner.read();
        let &idx = inner.index.get(oid)?;
        inner.entries.get(idx).map(|e| e.blob.clone())
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        let inner = self.inner.read();
        inner.index.contains_key(oid)
    }

    fn evict_old_entries(&self) {
        let now = Instant::now();
        let mut inner = self.inner.write();
        let mut evicted = false;

        while let Some(front) = inner.entries.front() {
            if now.duration_since(front.timestamp) > self.config.max_age {
                if let Some(removed) = inner.entries.pop_front() {
                    inner.index.remove(&removed.blob.oid);
                    self.current_size
                        .fetch_sub(removed.blob.data.len() as u64, Ordering::Relaxed);
                    evicted = true;
                }
            } else {
                break;
            }
        }

        if evicted {
            inner.rebuild_index();
        }
    }

    fn evict_if_needed(&self, incoming_size: usize) {
        let current = self.current_size.load(Ordering::Relaxed) as usize;
        let mut inner = self.inner.write();
        let mut evicted = false;

        while (current + incoming_size > self.config.max_size_bytes)
            || inner.entries.len() >= self.config.max_entries
        {
            if let Some(removed) = inner.entries.pop_front() {
                inner.index.remove(&removed.blob.oid);
                self.current_size
                    .fetch_sub(removed.blob.data.len() as u64, Ordering::Relaxed);
                evicted = true;
            } else {
                break;
            }
        }

        if evicted {
            inner.rebuild_index();
        }
    }

    pub fn drain(&self) -> Vec<Blob> {
        let mut inner = self.inner.write();
        let blobs: Vec<Blob> = inner.entries.drain(..).map(|e| e.blob).collect();
        inner.index.clear();
        self.current_size.store(0, Ordering::Relaxed);
        blobs
    }

    pub fn drain_older_than(&self, age: Duration) -> Vec<Blob> {
        let now = Instant::now();
        let mut inner = self.inner.write();
        let mut drained = Vec::new();

        while let Some(front) = inner.entries.front() {
            if now.duration_since(front.timestamp) > age {
                if let Some(removed) = inner.entries.pop_front() {
                    inner.index.remove(&removed.blob.oid);
                    self.current_size
                        .fetch_sub(removed.blob.data.len() as u64, Ordering::Relaxed);
                    drained.push(removed.blob);
                }
            } else {
                break;
            }
        }

        if !drained.is_empty() {
            inner.rebuild_index();
        }

        drained
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.entries.clear();
        inner.index.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().entries.is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.current_size.load(Ordering::Relaxed) as usize
    }

    pub fn oldest_entry_age(&self) -> Option<Duration> {
        let inner = self.inner.read();
        inner.entries.front().map(|e| e.timestamp.elapsed())
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new(WriteBufferConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blob(data: &[u8]) -> Blob {
        Blob::new(data.to_vec())
    }

    #[test]
    fn test_write_buffer_config_default() {
        let config = WriteBufferConfig::default();
        assert_eq!(config.max_entries, 1000);
        assert_eq!(config.max_size_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_buffer_add_get() {
        let buffer = WriteBuffer::default();
        let blob = create_test_blob(b"hello world");

        buffer.add(blob.clone());
        let retrieved = buffer.get(&blob.oid);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.as_ref(), b"hello world");
    }

    #[test]
    fn test_buffer_miss() {
        let buffer = WriteBuffer::default();
        let oid = Oid::hash(b"nonexistent");

        let result = buffer.get(&oid);
        assert!(result.is_none());
    }

    #[test]
    fn test_buffer_contains() {
        let buffer = WriteBuffer::default();
        let blob = create_test_blob(b"test");

        assert!(!buffer.contains(&blob.oid));
        buffer.add(blob.clone());
        assert!(buffer.contains(&blob.oid));
    }

    #[test]
    fn test_buffer_clear() {
        let buffer = WriteBuffer::default();
        let blob = create_test_blob(b"test");

        buffer.add(blob);
        assert!(!buffer.is_empty());

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.size_bytes(), 0);
    }

    #[test]
    fn test_buffer_len() {
        let buffer = WriteBuffer::default();
        assert_eq!(buffer.len(), 0);

        let blob = create_test_blob(b"test");
        buffer.add(blob);
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_buffer_drain() {
        let buffer = WriteBuffer::default();
        let blob1 = create_test_blob(b"test1");
        let blob2 = create_test_blob(b"test2");

        buffer.add(blob1);
        buffer.add(blob2);

        let drained = buffer.drain();
        assert_eq!(drained.len(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_eviction() {
        let config = WriteBufferConfig {
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let buffer = WriteBuffer::new(config);

        for i in 0..3u8 {
            let blob = create_test_blob(&[i; 10]);
            buffer.add(blob);
        }

        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_buffer_default() {
        let buffer = WriteBuffer::default();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_size_bytes() {
        let buffer = WriteBuffer::default();
        let blob = create_test_blob(b"12345678901234567890");
        buffer.add(blob);
        assert_eq!(buffer.size_bytes(), 20);
    }

    #[test]
    fn test_oldest_entry_age() {
        let buffer = WriteBuffer::default();
        assert!(buffer.oldest_entry_age().is_none());

        let blob = create_test_blob(b"test");
        buffer.add(blob);
        assert!(buffer.oldest_entry_age().is_some());
    }

    #[test]
    fn test_drain_older_than() {
        let buffer = WriteBuffer::default();
        let blob = create_test_blob(b"test");
        buffer.add(blob);

        let drained = buffer.drain_older_than(Duration::from_secs(3600));
        assert!(drained.is_empty());

        std::thread::sleep(Duration::from_millis(50));
        let drained = buffer.drain_older_than(Duration::from_millis(10));
        assert_eq!(drained.len(), 1);
    }

    #[test]
    fn test_buffer_duplicate_add() {
        let buffer = WriteBuffer::default();
        let blob = create_test_blob(b"test data");

        buffer.add(blob.clone());
        buffer.add(blob.clone());

        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_buffer_lookup_after_eviction() {
        let config = WriteBufferConfig {
            max_entries: 2,
            max_size_bytes: 1024 * 1024,
            max_age: Duration::from_secs(3600),
        };
        let buffer = WriteBuffer::new(config);

        let blob1 = create_test_blob(b"first");
        let blob2 = create_test_blob(b"second");
        let blob3 = create_test_blob(b"third");

        buffer.add(blob1.clone());
        buffer.add(blob2.clone());
        buffer.add(blob3.clone());

        assert!(!buffer.contains(&blob1.oid));
        assert!(buffer.contains(&blob2.oid));
        assert!(buffer.contains(&blob3.oid));

        assert!(buffer.get(&blob2.oid).is_some());
        assert!(buffer.get(&blob3.oid).is_some());
    }
}
