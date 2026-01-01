use std::fs::{File, OpenOptions};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::bucket::{DiskBucket, BUCKET_SIZE};
use crate::error::{BucketStoreError, Result};
use crate::io::AsyncMultiQueueIo;

pub struct BucketIo {
    fd: RawFd,
    _file: File,
    bucket_count: u32,
    path: PathBuf,
    io: Arc<AsyncMultiQueueIo>,
}

impl BucketIo {
    pub fn create(
        path: &Path,
        bucket_count: u32,
        io: Arc<AsyncMultiQueueIo>,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let total_size = bucket_count as u64 * BUCKET_SIZE as u64;
        file.set_len(total_size)?;

        let fd = file.as_raw_fd();

        Ok(Self {
            fd,
            _file: file,
            bucket_count,
            path: path.to_path_buf(),
            io,
        })
    }

    pub fn open(path: &Path, io: Arc<AsyncMultiQueueIo>) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let metadata = file.metadata()?;
        let bucket_count = (metadata.len() / BUCKET_SIZE as u64) as u32;

        let fd = file.as_raw_fd();

        Ok(Self {
            fd,
            _file: file,
            bucket_count,
            path: path.to_path_buf(),
            io,
        })
    }

    pub async fn read_bucket(&self, bucket_id: u32) -> Result<DiskBucket> {
        let offset = bucket_id as u64 * BUCKET_SIZE as u64;
        let queue = self.io.queue_for_bucket(bucket_id);

        let rx = queue.submit_read(self.fd, offset, BUCKET_SIZE)?;
        let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;

        result.result.map_err(BucketStoreError::from)?;
        let bytes: [u8; BUCKET_SIZE] = result
            .buffer
            .as_ref()
            .try_into()
            .map_err(|_| BucketStoreError::CorruptedBucket {
                offset,
                reason: "invalid buffer size".into(),
            })?;
        DiskBucket::from_bytes(&bytes)
    }

    pub async fn write_bucket(&self, bucket_id: u32, bucket: &DiskBucket) -> Result<()> {
        let offset = bucket_id as u64 * BUCKET_SIZE as u64;
        let data = Box::from(bucket.to_bytes());
        let queue = self.io.queue_for_bucket(bucket_id);

        let rx = queue.submit_write(self.fd, offset, data)?;
        let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;

        result.result.map_err(BucketStoreError::from)?;
        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        let mut receivers = Vec::with_capacity(self.io.queue_count());

        for i in 0..self.io.queue_count() {
            let queue = self.io.get_queue(i);
            let rx = queue.submit_fsync(self.fd)?;
            receivers.push(rx);
        }

        for rx in receivers {
            let result = rx.await.map_err(|_| BucketStoreError::IoCompletion)?;
            result.result.map_err(BucketStoreError::from)?;
        }

        Ok(())
    }

    pub fn bucket_count(&self) -> u32 {
        self.bucket_count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
