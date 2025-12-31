use std::collections::HashMap;
use std::os::unix::io::RawFd;

use io_uring::{opcode, types, IoUring};

use crate::error::{BitcaskError, Result};

pub const DEFAULT_RING_SIZE: u32 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOperation {
    Read,
    Write,
    Fsync,
}

pub struct IoRequest {
    pub id: u64,
    pub operation: IoOperation,
    pub fd: RawFd,
    pub offset: u64,
    pub buffer: Box<[u8]>,
}

pub struct IoCompletion {
    pub id: u64,
    pub result: std::io::Result<usize>,
    pub buffer: Box<[u8]>,
}

pub struct UringHandle {
    ring: IoUring,
    inflight: HashMap<u64, IoRequest>,
    next_id: u64,
}

impl UringHandle {
    pub fn new(ring_size: u32) -> Result<Self> {
        let ring = IoUring::new(ring_size).map_err(|e| BitcaskError::IoUring(e.to_string()))?;

        Ok(Self {
            ring,
            inflight: HashMap::with_capacity(ring_size as usize),
            next_id: 0,
        })
    }

    pub fn submit_read(&mut self, fd: RawFd, offset: u64, len: usize) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;

        let buffer = allocate_aligned(len, 4096);

        let read_e = opcode::Read::new(types::Fd(fd), buffer.as_ptr() as *mut u8, len as u32)
            .offset(offset)
            .build()
            .user_data(id);

        unsafe {
            self.ring
                .submission()
                .push(&read_e)
                .map_err(|e| BitcaskError::IoUring(format!("submission queue full: {:?}", e)))?;
        }

        self.inflight.insert(
            id,
            IoRequest {
                id,
                operation: IoOperation::Read,
                fd,
                offset,
                buffer,
            },
        );

        Ok(id)
    }

    pub fn submit_write(&mut self, fd: RawFd, offset: u64, data: Box<[u8]>) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;

        let write_e = opcode::Write::new(types::Fd(fd), data.as_ptr(), data.len() as u32)
            .offset(offset)
            .build()
            .user_data(id);

        unsafe {
            self.ring
                .submission()
                .push(&write_e)
                .map_err(|e| BitcaskError::IoUring(format!("submission queue full: {:?}", e)))?;
        }

        self.inflight.insert(
            id,
            IoRequest {
                id,
                operation: IoOperation::Write,
                fd,
                offset,
                buffer: data,
            },
        );

        Ok(id)
    }

    pub fn submit_fsync(&mut self, fd: RawFd) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;

        let fsync_e = opcode::Fsync::new(types::Fd(fd)).build().user_data(id);

        unsafe {
            self.ring
                .submission()
                .push(&fsync_e)
                .map_err(|e| BitcaskError::IoUring(format!("submission queue full: {:?}", e)))?;
        }

        self.inflight.insert(
            id,
            IoRequest {
                id,
                operation: IoOperation::Fsync,
                fd,
                offset: 0,
                buffer: Box::new([]),
            },
        );

        Ok(id)
    }

    pub fn submit_batch(&mut self) -> Result<usize> {
        self.ring
            .submit()
            .map_err(|e| BitcaskError::IoUring(e.to_string()))
    }

    pub fn wait_completions(&mut self, min_complete: usize) -> Vec<IoCompletion> {
        let mut completions = Vec::with_capacity(min_complete);

        if self.ring.submit_and_wait(min_complete).is_err() {
            return completions;
        }

        for cqe in self.ring.completion() {
            let id = cqe.user_data();
            let result = cqe.result();

            if let Some(req) = self.inflight.remove(&id) {
                let io_result = if result >= 0 {
                    Ok(result as usize)
                } else {
                    Err(std::io::Error::from_raw_os_error(-result))
                };

                completions.push(IoCompletion {
                    id,
                    result: io_result,
                    buffer: req.buffer,
                });
            }
        }

        completions
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }
}

fn allocate_aligned(size: usize, alignment: usize) -> Box<[u8]> {
    let aligned_size = (size + alignment - 1) & !(alignment - 1);
    vec![0u8; aligned_size].into_boxed_slice()
}
