use std::fs::{File, OpenOptions};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{BucketStoreError, Result};
use crate::io::AsyncMultiQueueIo;
use crate::record::{DataRecord, BLOCK_SIZE};

pub struct DataFile {
    fd: RawFd,
    _file: File,
    file_id: u16,
    path: PathBuf,
    offset: AtomicU64,
    io: Arc<AsyncMultiQueueIo>,
}

impl DataFile {
    pub fn create(path: &Path, file_id: u16, io: Arc<AsyncMultiQueueIo>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let fd = file.as_raw_fd();

        Ok(Self {
            fd,
            _file: file,
            file_id,
            path: path.to_path_buf(),
            offset: AtomicU64::new(0),
            io,
        })
    }

    pub fn open(path: &Path, file_id: u16, io: Arc<AsyncMultiQueueIo>) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let metadata = file.metadata()?;
        let offset = metadata.len();
        let fd = file.as_raw_fd();

        Ok(Self {
            fd,
            _file: file,
            file_id,
            path: path.to_path_buf(),
            offset: AtomicU64::new(offset),
            io,
        })
    }

    pub async fn append(&self, record: &DataRecord) -> Result<u64> {
        let data = record.to_bytes();
        let len = data.len() as u64;
        let offset = self.offset.fetch_add(len, Ordering::SeqCst);

        let queue = self.io.get_queue(self.file_id as usize % self.io.queue_count());
        let rx = queue.submit_write(self.fd, offset, data.into())?;
        let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;
        result.result.map_err(BucketStoreError::from)?;

        Ok(offset)
    }

    pub async fn read_at(&self, offset: u64, size: usize) -> Result<Bytes> {
        let aligned_size = (size + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1);

        let queue = self.io.get_queue(self.file_id as usize % self.io.queue_count());
        let rx = queue.submit_read(self.fd, offset, aligned_size)?;
        let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;
        result.result.map_err(BucketStoreError::from)?;

        let record = DataRecord::from_bytes(&result.buffer)?;
        Ok(record.value)
    }

    pub async fn read_record_at(&self, offset: u64, size: usize) -> Result<(gitstratum_core::Oid, Bytes)> {
        let aligned_size = (size + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1);

        let queue = self.io.get_queue(self.file_id as usize % self.io.queue_count());
        let rx = queue.submit_read(self.fd, offset, aligned_size)?;
        let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;
        result.result.map_err(BucketStoreError::from)?;

        let record = DataRecord::from_bytes(&result.buffer)?;
        Ok((record.oid, record.value))
    }

    pub async fn sync(&self) -> Result<()> {
        let queue = self.io.get_queue(self.file_id as usize % self.io.queue_count());
        let rx = queue.submit_fsync(self.fd)?;
        let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;
        result.result.map_err(BucketStoreError::from)?;
        Ok(())
    }

    pub fn file_id(&self) -> u16 {
        self.file_id
    }

    pub fn size(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_record_at_blocking(&self, offset: u64, size: usize) -> Result<(gitstratum_core::Oid, Bytes)> {
        use std::io::{Read, Seek, SeekFrom};
        use std::os::unix::io::FromRawFd;

        let aligned_size = (size + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1);

        let mut file = unsafe { std::fs::File::from_raw_fd(self.fd) };
        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; aligned_size];
        file.read_exact(&mut buffer)?;

        std::mem::forget(file);

        let record = DataRecord::from_bytes(&buffer)?;
        Ok((record.oid, record.value))
    }
}
