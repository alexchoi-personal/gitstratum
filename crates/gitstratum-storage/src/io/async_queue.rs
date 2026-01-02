use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::UringHandle;
use crate::error::Result;

pub struct IoResult {
    pub result: std::io::Result<usize>,
    pub buffer: Box<[u8]>,
}

struct PendingOp {
    tx: oneshot::Sender<IoResult>,
}

pub struct AsyncUringQueue {
    uring: Mutex<UringHandle>,
    pending: Mutex<HashMap<u64, PendingOp>>,
}

impl AsyncUringQueue {
    pub fn new(queue_depth: u32) -> Result<Self> {
        let uring = UringHandle::new(queue_depth)?;
        Ok(Self {
            uring: Mutex::new(uring),
            pending: Mutex::new(HashMap::new()),
        })
    }

    pub fn submit_read(
        &self,
        fd: RawFd,
        offset: u64,
        len: usize,
    ) -> Result<oneshot::Receiver<IoResult>> {
        let (tx, rx) = oneshot::channel();

        let id = {
            let mut uring = self.uring.lock();
            let id = uring.submit_read(fd, offset, len)?;
            uring.submit_batch()?;
            id
        };

        self.pending.lock().insert(id, PendingOp { tx });

        Ok(rx)
    }

    pub fn submit_write(
        &self,
        fd: RawFd,
        offset: u64,
        data: Box<[u8]>,
    ) -> Result<oneshot::Receiver<IoResult>> {
        let (tx, rx) = oneshot::channel();

        let id = {
            let mut uring = self.uring.lock();
            let id = uring.submit_write(fd, offset, data)?;
            uring.submit_batch()?;
            id
        };

        self.pending.lock().insert(id, PendingOp { tx });

        Ok(rx)
    }

    pub fn submit_fsync(&self, fd: RawFd) -> Result<oneshot::Receiver<IoResult>> {
        let (tx, rx) = oneshot::channel();

        let id = {
            let mut uring = self.uring.lock();
            let id = uring.submit_fsync(fd)?;
            uring.submit_batch()?;
            id
        };

        self.pending.lock().insert(id, PendingOp { tx });

        Ok(rx)
    }

    pub fn reap_completions(&self) {
        let completions = {
            let mut uring = self.uring.lock();
            uring.wait_completions(0)
        };

        let mut pending = self.pending.lock();
        for completion in completions {
            if let Some(op) = pending.remove(&completion.id) {
                let _ = op.tx.send(IoResult {
                    result: completion.result,
                    buffer: completion.buffer,
                });
            }
        }
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.lock().is_empty()
    }
}

pub struct IoQueueConfig {
    pub num_queues: usize,
    pub queue_depth: u32,
}

impl Default for IoQueueConfig {
    fn default() -> Self {
        Self {
            num_queues: 4,
            queue_depth: 256,
        }
    }
}

pub struct AsyncMultiQueueIo {
    queues: Vec<Arc<AsyncUringQueue>>,
    shutdown: Arc<AtomicBool>,
}

impl AsyncMultiQueueIo {
    pub fn new(config: IoQueueConfig) -> Result<Self> {
        let mut queues = Vec::with_capacity(config.num_queues);
        for _ in 0..config.num_queues {
            queues.push(Arc::new(AsyncUringQueue::new(config.queue_depth)?));
        }
        Ok(Self {
            queues,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn start_reapers(&self) -> Vec<JoinHandle<()>> {
        self.queues
            .iter()
            .map(|queue| {
                let queue = Arc::clone(queue);
                let shutdown = Arc::clone(&self.shutdown);
                tokio::spawn(async move {
                    while !shutdown.load(Ordering::Relaxed) {
                        queue.reap_completions();
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    pub fn queue_for_bucket(&self, bucket_id: u32) -> &Arc<AsyncUringQueue> {
        &self.queues[(bucket_id as usize) % self.queues.len()]
    }

    pub fn queue_count(&self) -> usize {
        self.queues.len()
    }

    pub fn get_queue(&self, index: usize) -> &Arc<AsyncUringQueue> {
        &self.queues[index % self.queues.len()]
    }
}
