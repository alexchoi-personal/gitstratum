use std::os::unix::io::RawFd;

pub struct BatchRequest {
    pub fd: RawFd,
    pub offset: u64,
    pub data: Option<Box<[u8]>>,
    pub len: usize,
    pub is_write: bool,
}

pub struct BatchBuilder {
    requests: Vec<BatchRequest>,
}

impl BatchBuilder {
    pub fn new() -> Self {
        Self {
            requests: Vec::with_capacity(64),
        }
    }

    pub fn add_read(&mut self, fd: RawFd, offset: u64, len: usize) -> &mut Self {
        self.requests.push(BatchRequest {
            fd,
            offset,
            data: None,
            len,
            is_write: false,
        });
        self
    }

    pub fn add_write(&mut self, fd: RawFd, offset: u64, data: Box<[u8]>) -> &mut Self {
        let len = data.len();
        self.requests.push(BatchRequest {
            fd,
            offset,
            data: Some(data),
            len,
            is_write: true,
        });
        self
    }

    pub fn build(self) -> Vec<BatchRequest> {
        self.requests
    }

    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}
