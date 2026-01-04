use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::bucket::{DiskBucket, BUCKET_SIZE};
use crate::error::{BucketStoreError, Result};

pub struct BucketFile {
    file: File,
    bucket_count: u32,
    path: PathBuf,
}

impl BucketFile {
    pub fn try_clone(&self) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        Ok(Self {
            file,
            bucket_count: self.bucket_count,
            path: self.path.clone(),
        })
    }

    pub fn create(path: &Path, bucket_count: u32) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let total_size = bucket_count as u64 * BUCKET_SIZE as u64;
        file.set_len(total_size)?;

        Ok(Self {
            file,
            bucket_count,
            path: path.to_path_buf(),
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let bucket_count_u64 = file_size / BUCKET_SIZE as u64;
        let bucket_count =
            u32::try_from(bucket_count_u64).map_err(|_| BucketStoreError::FileTooLarge {
                size: file_size,
                max: u32::MAX as u64 * BUCKET_SIZE as u64,
            })?;

        Ok(Self {
            file,
            bucket_count,
            path: path.to_path_buf(),
        })
    }

    pub fn read_bucket(&mut self, bucket_id: u32) -> Result<DiskBucket> {
        if bucket_id >= self.bucket_count {
            return Err(BucketStoreError::InvalidBucketId {
                bucket_id,
                bucket_count: self.bucket_count,
            });
        }
        let offset = bucket_id as u64 * BUCKET_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buf = [0u8; BUCKET_SIZE];
        self.file.read_exact(&mut buf)?;

        DiskBucket::from_bytes(&buf)
    }

    pub fn write_bucket(&mut self, bucket_id: u32, bucket: &DiskBucket) -> Result<()> {
        if bucket_id >= self.bucket_count {
            return Err(BucketStoreError::InvalidBucketId {
                bucket_id,
                bucket_count: self.bucket_count,
            });
        }
        let offset = bucket_id as u64 * BUCKET_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let buf = bucket.to_bytes();
        self.file.write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn bucket_count(&self) -> u32 {
        self.bucket_count
    }
}
