use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use gitstratum_core::{Oid, RepoId};

#[derive(Debug, Clone)]
pub struct PrecomputeRequest {
    pub repo_id: RepoId,
    pub wants: Vec<Oid>,
    pub haves: Vec<Oid>,
    pub priority: PrecomputePriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum PrecomputePriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

pub struct PrecomputeQueue {
    queue: RwLock<Vec<PrecomputeRequest>>,
    processing: RwLock<HashSet<String>>,
    running: AtomicBool,
}

impl PrecomputeQueue {
    pub fn new() -> Self {
        Self {
            queue: RwLock::new(Vec::new()),
            processing: RwLock::new(HashSet::new()),
            running: AtomicBool::new(false),
        }
    }

    pub fn enqueue(&self, request: PrecomputeRequest) -> bool {
        let key = self.request_key(&request);

        if self.processing.read().contains(&key) {
            return false;
        }

        let mut queue = self.queue.write();

        if queue.iter().any(|r| self.request_key(r) == key) {
            return false;
        }

        let insert_idx = queue
            .iter()
            .position(|r| r.priority < request.priority)
            .unwrap_or(queue.len());

        queue.insert(insert_idx, request);
        true
    }

    pub fn dequeue(&self) -> Option<PrecomputeRequest> {
        let mut queue = self.queue.write();
        if let Some(request) = queue.pop() {
            let key = self.request_key(&request);
            self.processing.write().insert(key);
            Some(request)
        } else {
            None
        }
    }

    pub fn complete(&self, request: &PrecomputeRequest) {
        let key = self.request_key(request);
        self.processing.write().remove(&key);
    }

    pub fn fail(&self, request: &PrecomputeRequest) {
        let key = self.request_key(request);
        self.processing.write().remove(&key);
    }

    pub fn queue_size(&self) -> usize {
        self.queue.read().len()
    }

    pub fn processing_count(&self) -> usize {
        self.processing.read().len()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn request_key(&self, request: &PrecomputeRequest) -> String {
        let mut wants = request.wants.clone();
        let mut haves = request.haves.clone();
        wants.sort();
        haves.sort();

        let wants_str: Vec<String> = wants.iter().map(|o| o.to_hex()).collect();
        let haves_str: Vec<String> = haves.iter().map(|o| o.to_hex()).collect();

        format!(
            "{}:{}:{}",
            request.repo_id.as_str(),
            wants_str.join(","),
            haves_str.join(",")
        )
    }

    pub fn clear(&self) {
        self.queue.write().clear();
    }
}

impl Default for PrecomputeQueue {
    fn default() -> Self {
        Self::new()
    }
}

pub trait PrecomputeHandler: Send + Sync {
    fn handle(&self, request: &PrecomputeRequest) -> Result<(), String>;
}

pub struct PrecomputeWorker<H: PrecomputeHandler> {
    queue: Arc<PrecomputeQueue>,
    handler: Arc<H>,
}

impl<H: PrecomputeHandler> PrecomputeWorker<H> {
    pub fn new(queue: Arc<PrecomputeQueue>, handler: Arc<H>) -> Self {
        Self { queue, handler }
    }

    pub fn process_one(&self) -> bool {
        if let Some(request) = self.queue.dequeue() {
            match self.handler.handle(&request) {
                Ok(()) => {
                    self.queue.complete(&request);
                    true
                }
                Err(_) => {
                    self.queue.fail(&request);
                    false
                }
            }
        } else {
            false
        }
    }

    pub fn process_all(&self) -> usize {
        let mut count = 0;
        while self.queue.is_running() {
            if self.process_one() {
                count += 1;
            } else {
                break;
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precompute_priority_ordering() {
        assert!(PrecomputePriority::Low < PrecomputePriority::Normal);
        assert!(PrecomputePriority::Normal < PrecomputePriority::High);
        assert!(PrecomputePriority::High < PrecomputePriority::Critical);
    }

    #[test]
    fn test_precompute_priority_default() {
        assert_eq!(PrecomputePriority::default(), PrecomputePriority::Normal);
    }

    #[test]
    fn test_precompute_queue_enqueue_dequeue() {
        let queue = PrecomputeQueue::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        let request = PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        };

        assert!(queue.enqueue(request.clone()));
        assert_eq!(queue.queue_size(), 1);

        let dequeued = queue.dequeue().unwrap();
        assert_eq!(dequeued.repo_id.as_str(), "test/repo");
        assert_eq!(queue.processing_count(), 1);
    }

    #[test]
    fn test_precompute_queue_priority() {
        let queue = PrecomputeQueue::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        queue.enqueue(PrecomputeRequest {
            repo_id: repo_id.clone(),
            wants: vec![Oid::hash(b"low")],
            haves: vec![],
            priority: PrecomputePriority::Low,
        });

        queue.enqueue(PrecomputeRequest {
            repo_id: repo_id.clone(),
            wants: vec![Oid::hash(b"high")],
            haves: vec![],
            priority: PrecomputePriority::High,
        });

        queue.enqueue(PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"normal")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        });

        let first = queue.dequeue().unwrap();
        assert_eq!(first.priority, PrecomputePriority::Low);

        let second = queue.dequeue().unwrap();
        assert_eq!(second.priority, PrecomputePriority::Normal);

        let third = queue.dequeue().unwrap();
        assert_eq!(third.priority, PrecomputePriority::High);
    }

    #[test]
    fn test_precompute_queue_no_duplicates() {
        let queue = PrecomputeQueue::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        let request = PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        };

        assert!(queue.enqueue(request.clone()));
        assert!(!queue.enqueue(request));
        assert_eq!(queue.queue_size(), 1);
    }

    #[test]
    fn test_precompute_queue_complete() {
        let queue = PrecomputeQueue::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        let request = PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        };

        queue.enqueue(request.clone());
        let dequeued = queue.dequeue().unwrap();
        assert_eq!(queue.processing_count(), 1);

        queue.complete(&dequeued);
        assert_eq!(queue.processing_count(), 0);
    }

    #[test]
    fn test_precompute_queue_fail() {
        let queue = PrecomputeQueue::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        let request = PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        };

        queue.enqueue(request.clone());
        let dequeued = queue.dequeue().unwrap();
        queue.fail(&dequeued);
        assert_eq!(queue.processing_count(), 0);
    }

    #[test]
    fn test_precompute_queue_running() {
        let queue = PrecomputeQueue::new();

        assert!(!queue.is_running());
        queue.start();
        assert!(queue.is_running());
        queue.stop();
        assert!(!queue.is_running());
    }

    #[test]
    fn test_precompute_queue_clear() {
        let queue = PrecomputeQueue::new();
        let repo_id = RepoId::new("test/repo").unwrap();

        queue.enqueue(PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        });

        queue.clear();
        assert_eq!(queue.queue_size(), 0);
    }

    struct MockHandler {
        succeed: bool,
    }

    impl PrecomputeHandler for MockHandler {
        fn handle(&self, _request: &PrecomputeRequest) -> Result<(), String> {
            if self.succeed {
                Ok(())
            } else {
                Err("failed".to_string())
            }
        }
    }

    #[test]
    fn test_precompute_worker_process_one() {
        let queue = Arc::new(PrecomputeQueue::new());
        let handler = Arc::new(MockHandler { succeed: true });
        let worker = PrecomputeWorker::new(queue.clone(), handler);

        let repo_id = RepoId::new("test/repo").unwrap();
        queue.enqueue(PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        });

        assert!(worker.process_one());
        assert_eq!(queue.queue_size(), 0);
        assert_eq!(queue.processing_count(), 0);
    }

    #[test]
    fn test_precompute_worker_process_one_failure() {
        let queue = Arc::new(PrecomputeQueue::new());
        let handler = Arc::new(MockHandler { succeed: false });
        let worker = PrecomputeWorker::new(queue.clone(), handler);

        let repo_id = RepoId::new("test/repo").unwrap();
        queue.enqueue(PrecomputeRequest {
            repo_id,
            wants: vec![Oid::hash(b"want")],
            haves: vec![],
            priority: PrecomputePriority::Normal,
        });

        assert!(!worker.process_one());
        assert_eq!(queue.processing_count(), 0);
    }
}
