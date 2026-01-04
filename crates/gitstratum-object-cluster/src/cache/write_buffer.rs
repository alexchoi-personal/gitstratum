use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gitstratum_core::{Blob, Oid};
use parking_lot::RwLock;

#[derive(Clone)]
struct BufferEntry {
    blob: Arc<Blob>,
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
    order: VecDeque<Oid>,
    blobs: HashMap<Oid, BufferEntry>,
}

impl BufferInner {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            blobs: HashMap::new(),
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
        let now = Instant::now();

        let entry = BufferEntry {
            blob: Arc::new(blob),
            timestamp: now,
        };

        let mut inner = self.inner.write();

        if inner.blobs.contains_key(&oid) {
            return;
        }

        self.evict_old_entries_locked(&mut inner, now);
        self.evict_if_needed_locked(&mut inner, size);

        inner.order.push_back(oid);
        inner.blobs.insert(oid, entry);
        self.current_size.fetch_add(size as u64, Ordering::Relaxed);
    }

    pub fn get(&self, oid: &Oid) -> Option<Arc<Blob>> {
        let inner = self.inner.read();
        inner.blobs.get(oid).map(|e| Arc::clone(&e.blob))
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        let inner = self.inner.read();
        inner.blobs.contains_key(oid)
    }

    fn evict_old_entries_locked(&self, inner: &mut BufferInner, now: Instant) {
        while let Some(&front_oid) = inner.order.front() {
            if let Some(entry) = inner.blobs.get(&front_oid) {
                if now.duration_since(entry.timestamp) > self.config.max_age {
                    inner.order.pop_front();
                    if let Some(removed) = inner.blobs.remove(&front_oid) {
                        self.current_size
                            .fetch_sub(removed.blob.data.len() as u64, Ordering::Relaxed);
                    }
                } else {
                    break;
                }
            } else {
                inner.order.pop_front();
            }
        }
    }

    fn evict_if_needed_locked(&self, inner: &mut BufferInner, incoming_size: usize) {
        while (self.current_size.load(Ordering::Relaxed) as usize + incoming_size
            > self.config.max_size_bytes)
            || inner.blobs.len() >= self.config.max_entries
        {
            if let Some(front_oid) = inner.order.pop_front() {
                if let Some(removed) = inner.blobs.remove(&front_oid) {
                    self.current_size
                        .fetch_sub(removed.blob.data.len() as u64, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }
    }

    pub fn drain(&self) -> Vec<Arc<Blob>> {
        let mut inner = self.inner.write();
        let blobs: Vec<Arc<Blob>> = inner.blobs.drain().map(|(_, e)| e.blob).collect();
        inner.order.clear();
        self.current_size.store(0, Ordering::Relaxed);
        blobs
    }

    pub fn drain_older_than(&self, age: Duration) -> Vec<Arc<Blob>> {
        let now = Instant::now();
        let mut inner = self.inner.write();
        let mut drained = Vec::new();

        while let Some(&front_oid) = inner.order.front() {
            if let Some(entry) = inner.blobs.get(&front_oid) {
                if now.duration_since(entry.timestamp) > age {
                    inner.order.pop_front();
                    if let Some(removed) = inner.blobs.remove(&front_oid) {
                        self.current_size
                            .fetch_sub(removed.blob.data.len() as u64, Ordering::Relaxed);
                        drained.push(removed.blob);
                    }
                } else {
                    break;
                }
            } else {
                inner.order.pop_front();
            }
        }

        drained
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.order.clear();
        inner.blobs.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.inner.read().blobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().blobs.is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.current_size.load(Ordering::Relaxed) as usize
    }

    pub fn oldest_entry_age(&self) -> Option<Duration> {
        let inner = self.inner.read();
        inner
            .order
            .front()
            .and_then(|oid| inner.blobs.get(oid))
            .map(|e| e.timestamp.elapsed())
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
        assert_eq!(retrieved.as_ref().unwrap().data.as_ref(), b"hello world");
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
