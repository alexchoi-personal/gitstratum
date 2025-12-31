use bytes::{Bytes, BytesMut};
use gitstratum_core::Object;
use tokio::sync::mpsc;

use crate::error::Result;
use super::assembly::{PackEntry, PackWriter};

#[derive(Debug, Clone)]
pub struct PackStreamConfig {
    pub chunk_size: usize,
    pub max_objects_per_chunk: usize,
}

impl PackStreamConfig {
    pub fn new() -> Self {
        Self {
            chunk_size: 64 * 1024,
            max_objects_per_chunk: 100,
        }
    }

    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    pub fn with_max_objects(mut self, max: usize) -> Self {
        self.max_objects_per_chunk = max;
        self
    }
}

impl Default for PackStreamConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StreamingPackWriter {
    config: PackStreamConfig,
    current_writer: PackWriter,
    current_size: usize,
    object_count: usize,
}

impl StreamingPackWriter {
    pub fn new(config: PackStreamConfig) -> Self {
        Self {
            config,
            current_writer: PackWriter::new(),
            current_size: 0,
            object_count: 0,
        }
    }

    pub fn add_object(&mut self, obj: &Object) -> Result<Option<Bytes>> {
        let entry = PackEntry::from_object(obj)?;
        let entry_size = entry.data.len();

        let should_flush = self.current_size + entry_size > self.config.chunk_size
            || self.object_count >= self.config.max_objects_per_chunk;

        let flushed = if should_flush && self.object_count > 0 {
            Some(self.flush_current()?)
        } else {
            None
        };

        self.current_writer.add_entry(entry);
        self.current_size += entry_size;
        self.object_count += 1;

        Ok(flushed)
    }

    fn flush_current(&mut self) -> Result<Bytes> {
        let old_writer = std::mem::replace(&mut self.current_writer, PackWriter::new());
        self.current_size = 0;
        self.object_count = 0;
        old_writer.build()
    }

    pub fn finish(self) -> Result<Option<Bytes>> {
        if self.object_count > 0 {
            Ok(Some(self.current_writer.build()?))
        } else {
            Ok(None)
        }
    }

    pub fn pending_count(&self) -> usize {
        self.object_count
    }
}

pub async fn stream_objects_to_channel(
    objects: Vec<Object>,
    config: PackStreamConfig,
    tx: mpsc::Sender<Result<Bytes>>,
) -> Result<()> {
    let mut writer = StreamingPackWriter::new(config);

    for obj in objects {
        if let Some(chunk) = writer.add_object(&obj)? {
            if tx.send(Ok(chunk)).await.is_err() {
                return Ok(());
            }
        }
    }

    if let Some(final_chunk) = writer.finish()? {
        let _ = tx.send(Ok(final_chunk)).await;
    }

    Ok(())
}

pub struct PackStreamBuffer {
    buffer: BytesMut,
    config: PackStreamConfig,
}

impl PackStreamBuffer {
    pub fn new(config: PackStreamConfig) -> Self {
        Self {
            buffer: BytesMut::with_capacity(config.chunk_size),
            config,
        }
    }

    pub fn write(&mut self, data: &[u8]) -> Option<Bytes> {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() >= self.config.chunk_size {
            let chunk = self.buffer.split().freeze();
            Some(chunk)
        } else {
            None
        }
    }

    pub fn finish(self) -> Option<Bytes> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(self.buffer.freeze())
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitstratum_core::Blob;

    #[test]
    fn test_pack_stream_config_new() {
        let config = PackStreamConfig::new();
        assert_eq!(config.chunk_size, 64 * 1024);
        assert_eq!(config.max_objects_per_chunk, 100);
    }

    #[test]
    fn test_pack_stream_config_default() {
        let config = PackStreamConfig::default();
        assert_eq!(config.chunk_size, 64 * 1024);
    }

    #[test]
    fn test_pack_stream_config_with_chunk_size() {
        let config = PackStreamConfig::new().with_chunk_size(1024);
        assert_eq!(config.chunk_size, 1024);
    }

    #[test]
    fn test_pack_stream_config_with_max_objects() {
        let config = PackStreamConfig::new().with_max_objects(50);
        assert_eq!(config.max_objects_per_chunk, 50);
    }

    #[test]
    fn test_streaming_pack_writer_new() {
        let config = PackStreamConfig::new();
        let writer = StreamingPackWriter::new(config);
        assert_eq!(writer.pending_count(), 0);
    }

    #[test]
    fn test_streaming_pack_writer_add_object() {
        let config = PackStreamConfig::new().with_chunk_size(1024 * 1024);
        let mut writer = StreamingPackWriter::new(config);

        let blob = Blob::new(b"test content".to_vec());
        let obj = Object::Blob(blob);

        let result = writer.add_object(&obj).unwrap();
        assert!(result.is_none());
        assert_eq!(writer.pending_count(), 1);
    }

    #[test]
    fn test_streaming_pack_writer_flush_on_size() {
        let config = PackStreamConfig::new().with_chunk_size(10);
        let mut writer = StreamingPackWriter::new(config);

        let blob1 = Blob::new(b"content 1".to_vec());
        let blob2 = Blob::new(b"content 2".to_vec());

        writer.add_object(&Object::Blob(blob1)).unwrap();
        let result = writer.add_object(&Object::Blob(blob2)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_streaming_pack_writer_flush_on_count() {
        let config = PackStreamConfig::new().with_max_objects(1);
        let mut writer = StreamingPackWriter::new(config);

        let blob1 = Blob::new(b"a".to_vec());
        let blob2 = Blob::new(b"b".to_vec());

        writer.add_object(&Object::Blob(blob1)).unwrap();
        let result = writer.add_object(&Object::Blob(blob2)).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_streaming_pack_writer_finish() {
        let config = PackStreamConfig::new();
        let mut writer = StreamingPackWriter::new(config);

        let blob = Blob::new(b"test".to_vec());
        writer.add_object(&Object::Blob(blob)).unwrap();

        let result = writer.finish().unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_streaming_pack_writer_finish_empty() {
        let config = PackStreamConfig::new();
        let writer = StreamingPackWriter::new(config);

        let result = writer.finish().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_pack_stream_buffer_new() {
        let config = PackStreamConfig::new();
        let buffer = PackStreamBuffer::new(config);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_pack_stream_buffer_write() {
        let config = PackStreamConfig::new().with_chunk_size(10);
        let mut buffer = PackStreamBuffer::new(config);

        let result = buffer.write(b"short");
        assert!(result.is_none());
        assert_eq!(buffer.len(), 5);

        let result = buffer.write(b"more data");
        assert!(result.is_some());
    }

    #[test]
    fn test_pack_stream_buffer_finish() {
        let config = PackStreamConfig::new();
        let mut buffer = PackStreamBuffer::new(config);

        buffer.write(b"data");
        let result = buffer.finish();
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), b"data");
    }

    #[test]
    fn test_pack_stream_buffer_finish_empty() {
        let config = PackStreamConfig::new();
        let buffer = PackStreamBuffer::new(config);
        let result = buffer.finish();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_stream_objects_to_channel() {
        let (tx, mut rx) = mpsc::channel(10);
        let config = PackStreamConfig::new();

        let blob = Blob::new(b"test".to_vec());
        let objects = vec![Object::Blob(blob)];

        tokio::spawn(async move {
            stream_objects_to_channel(objects, config, tx).await.unwrap();
        });

        let mut received = Vec::new();
        while let Some(result) = rx.recv().await {
            received.push(result.unwrap());
        }

        assert_eq!(received.len(), 1);
    }
}
