use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;

use super::{IoCompletion, UringHandle, DEFAULT_RING_SIZE};
use crate::error::Result;

pub struct IoQueueConfig {
    pub num_queues: usize,
    pub queue_depth: u32,
}

impl Default for IoQueueConfig {
    fn default() -> Self {
        Self {
            num_queues: 4,
            queue_depth: DEFAULT_RING_SIZE,
        }
    }
}

pub struct MultiQueueIo {
    queues: Vec<Arc<Mutex<UringHandle>>>,
    _completion_rx: Receiver<IoCompletion>,
    _completion_tx: Sender<IoCompletion>,
}

impl MultiQueueIo {
    pub fn new(config: IoQueueConfig) -> Result<Self> {
        let mut queues = Vec::with_capacity(config.num_queues);

        for _ in 0..config.num_queues {
            let handle = UringHandle::new(config.queue_depth)?;
            queues.push(Arc::new(Mutex::new(handle)));
        }

        let (completion_tx, completion_rx) =
            bounded(config.queue_depth as usize * config.num_queues);

        Ok(Self {
            queues,
            _completion_rx: completion_rx,
            _completion_tx: completion_tx,
        })
    }

    pub fn queue_for_bucket(&self, bucket_id: u32) -> Arc<Mutex<UringHandle>> {
        let queue_idx = (bucket_id as usize) % self.queues.len();
        Arc::clone(&self.queues[queue_idx])
    }

    pub fn queue_count(&self) -> usize {
        self.queues.len()
    }

    pub fn get_queue(&self, index: usize) -> Arc<Mutex<UringHandle>> {
        Arc::clone(&self.queues[index % self.queues.len()])
    }
}
