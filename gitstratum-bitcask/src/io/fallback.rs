use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::{FromRawFd, RawFd};

use crate::error::Result;

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
    pending: Vec<IoRequest>,
    next_id: u64,
}

impl UringHandle {
    pub fn new(_ring_size: u32) -> Result<Self> {
        Ok(Self {
            pending: Vec::with_capacity(64),
            next_id: 0,
        })
    }

    pub fn submit_read(&mut self, fd: RawFd, offset: u64, len: usize) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;

        let buffer = allocate_aligned(len, 4096);

        self.pending.push(IoRequest {
            id,
            operation: IoOperation::Read,
            fd,
            offset,
            buffer,
        });

        Ok(id)
    }

    pub fn submit_write(&mut self, fd: RawFd, offset: u64, data: Box<[u8]>) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;

        self.pending.push(IoRequest {
            id,
            operation: IoOperation::Write,
            fd,
            offset,
            buffer: data,
        });

        Ok(id)
    }

    pub fn submit_fsync(&mut self, fd: RawFd) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;

        self.pending.push(IoRequest {
            id,
            operation: IoOperation::Fsync,
            fd,
            offset: 0,
            buffer: Box::new([]),
        });

        Ok(id)
    }

    pub fn submit_batch(&mut self) -> Result<usize> {
        Ok(self.pending.len())
    }

    pub fn wait_completions(&mut self, _min_complete: usize) -> Vec<IoCompletion> {
        let requests = std::mem::take(&mut self.pending);
        let mut completions = Vec::with_capacity(requests.len());

        for req in requests {
            let completion = match req.operation {
                IoOperation::Read => self.execute_read(req),
                IoOperation::Write => self.execute_write(req),
                IoOperation::Fsync => self.execute_fsync(req),
            };
            completions.push(completion);
        }

        completions
    }

    fn execute_read(&self, req: IoRequest) -> IoCompletion {
        let mut file = unsafe { File::from_raw_fd(req.fd) };
        let mut buffer = req.buffer.into_vec();

        let result = (|| {
            file.seek(SeekFrom::Start(req.offset))?;
            file.read(&mut buffer)
        })();

        std::mem::forget(file);

        IoCompletion {
            id: req.id,
            result,
            buffer: buffer.into_boxed_slice(),
        }
    }

    fn execute_write(&self, req: IoRequest) -> IoCompletion {
        let mut file = unsafe { File::from_raw_fd(req.fd) };
        let buffer = req.buffer;

        let result = (|| {
            file.seek(SeekFrom::Start(req.offset))?;
            file.write(&buffer)
        })();

        std::mem::forget(file);

        IoCompletion {
            id: req.id,
            result,
            buffer,
        }
    }

    fn execute_fsync(&self, req: IoRequest) -> IoCompletion {
        let file = unsafe { File::from_raw_fd(req.fd) };
        let result = file.sync_all().map(|_| 0);
        std::mem::forget(file);

        IoCompletion {
            id: req.id,
            result,
            buffer: req.buffer,
        }
    }

    pub fn inflight_count(&self) -> usize {
        self.pending.len()
    }
}

fn allocate_aligned(size: usize, alignment: usize) -> Box<[u8]> {
    let aligned_size = (size + alignment - 1) & !(alignment - 1);
    vec![0u8; aligned_size].into_boxed_slice()
}
