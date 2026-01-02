use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use super::data_file::DataFile;
use crate::error::Result;
use crate::io::AsyncMultiQueueIo;

pub struct FileManager {
    data_dir: PathBuf,
    next_file_id: AtomicU16,
    data_files: RwLock<HashMap<u16, PathBuf>>,
    io: Arc<AsyncMultiQueueIo>,
}

impl FileManager {
    pub fn new(data_dir: &Path, io: Arc<AsyncMultiQueueIo>) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        let mut max_file_id = 0u16;
        let mut data_files = HashMap::new();

        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "data" {
                    if let Some(stem) = path.file_stem() {
                        if let Ok(file_id) = stem.to_string_lossy().parse::<u16>() {
                            data_files.insert(file_id, path);
                            max_file_id = max_file_id.max(file_id);
                        }
                    }
                }
            }
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            next_file_id: AtomicU16::new(max_file_id + 1),
            data_files: RwLock::new(data_files),
            io,
        })
    }

    pub fn create_data_file(&self) -> Result<DataFile> {
        let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        let path = self.data_file_path(file_id);

        let file = DataFile::create(&path, file_id, self.io.clone())?;

        self.data_files.write().insert(file_id, path);

        Ok(file)
    }

    pub fn open_data_file(&self, file_id: u16) -> Result<DataFile> {
        let path = self.data_file_path(file_id);
        DataFile::open(&path, file_id, self.io.clone())
    }

    pub fn data_file_path(&self, file_id: u16) -> PathBuf {
        self.data_dir.join(format!("{:05}.data", file_id))
    }

    pub fn bucket_file_path(&self) -> PathBuf {
        self.data_dir.join("buckets.idx")
    }

    pub fn file_count(&self) -> usize {
        self.data_files.read().len()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}
