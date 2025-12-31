use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::{Read, Write};

use crate::error::{ObjectStoreError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Zlib,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Zlib
    }
}

#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub compression_type: CompressionType,
    pub level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::Zlib,
            level: 6,
        }
    }
}

pub struct Compressor {
    config: CompressionConfig,
}

impl Compressor {
    pub fn new(config: CompressionConfig) -> Self {
        Self { config }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression_type {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Zlib => self.compress_zlib(data),
        }
    }

    pub fn decompress(&self, compressed: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression_type {
            CompressionType::None => Ok(compressed.to_vec()),
            CompressionType::Zlib => self.decompress_zlib(compressed),
        }
    }

    fn compress_zlib(&self, data: &[u8]) -> Result<Vec<u8>> {
        let compression = Compression::new(self.config.level);
        let mut encoder = ZlibEncoder::new(Vec::new(), compression);
        encoder
            .write_all(data)
            .map_err(|e| ObjectStoreError::Compression(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| ObjectStoreError::Compression(e.to_string()))
    }

    fn decompress_zlib(&self, compressed: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = ZlibDecoder::new(compressed);
        let mut data = Vec::new();
        decoder
            .read_to_end(&mut data)
            .map_err(|e| ObjectStoreError::Decompression(e.to_string()))?;
        Ok(data)
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new(CompressionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zlib_compression() {
        let compressor = Compressor::default();
        let data = b"hello world hello world hello world";
        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_no_compression() {
        let compressor = Compressor::new(CompressionConfig {
            compression_type: CompressionType::None,
            level: 0,
        });
        let data = b"test data";
        let compressed = compressor.compress(data).unwrap();
        assert_eq!(data.as_slice(), compressed.as_slice());
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compression_type_default() {
        let compression_type = CompressionType::default();
        assert_eq!(compression_type, CompressionType::Zlib);
    }

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert_eq!(config.compression_type, CompressionType::Zlib);
        assert_eq!(config.level, 6);
    }

    #[test]
    fn test_compressor_default() {
        let compressor = Compressor::default();
        assert_eq!(compressor.config.compression_type, CompressionType::Zlib);
    }
}
