use std::os::fd::{BorrowedFd, RawFd};

use nix::sys::uio::{pread, pwrite};
use nix::unistd::fsync;

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
        let mut buffer = req.buffer.into_vec();

        let offset = match i64::try_from(req.offset) {
            Ok(o) => o,
            Err(_) => {
                return IoCompletion {
                    id: req.id,
                    result: Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "offset exceeds i64::MAX",
                    )),
                    buffer: buffer.into_boxed_slice(),
                };
            }
        };

        let fd = unsafe { BorrowedFd::borrow_raw(req.fd) };
        let result =
            pread(fd, &mut buffer, offset).map_err(|e| std::io::Error::from_raw_os_error(e as i32));

        IoCompletion {
            id: req.id,
            result,
            buffer: buffer.into_boxed_slice(),
        }
    }

    fn execute_write(&self, req: IoRequest) -> IoCompletion {
        let buffer = req.buffer;

        let offset = match i64::try_from(req.offset) {
            Ok(o) => o,
            Err(_) => {
                return IoCompletion {
                    id: req.id,
                    result: Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "offset exceeds i64::MAX",
                    )),
                    buffer,
                };
            }
        };

        let fd = unsafe { BorrowedFd::borrow_raw(req.fd) };
        let result =
            pwrite(fd, &buffer, offset).map_err(|e| std::io::Error::from_raw_os_error(e as i32));

        IoCompletion {
            id: req.id,
            result,
            buffer,
        }
    }

    fn execute_fsync(&self, req: IoRequest) -> IoCompletion {
        let result = fsync(req.fd)
            .map(|_| 0)
            .map_err(|e| std::io::Error::from_raw_os_error(e as i32));

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
