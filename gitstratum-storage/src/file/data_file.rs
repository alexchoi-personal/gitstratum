use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::error::Result;
use crate::record::{DataRecord, BLOCK_SIZE};

pub struct DataFile {
    file: File,
    file_id: u16,
    path: PathBuf,
    offset: AtomicU64,
}

impl DataFile {
    pub fn create(path: &Path, file_id: u16) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            file,
            file_id,
            path: path.to_path_buf(),
            offset: AtomicU64::new(0),
        })
    }

    pub fn open(path: &Path, file_id: u16) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let metadata = file.metadata()?;
        let offset = metadata.len();

        Ok(Self {
            file,
            file_id,
            path: path.to_path_buf(),
            offset: AtomicU64::new(offset),
        })
    }

    pub fn append(&mut self, record: &DataRecord) -> Result<u64> {
        let data = record.to_bytes();
        let offset = self.offset.load(Ordering::SeqCst);

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&data)?;

        self.offset.fetch_add(data.len() as u64, Ordering::SeqCst);

        Ok(offset)
    }

    pub fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes> {
        self.file.seek(SeekFrom::Start(offset))?;

        let aligned_size = (size + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1);
        let mut buf = vec![0u8; aligned_size];
        self.file.read_exact(&mut buf)?;

        let record = DataRecord::from_bytes(&buf)?;
        Ok(record.value)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
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
}
