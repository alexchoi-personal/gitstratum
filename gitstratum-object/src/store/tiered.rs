use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    Hot,
    Cold,
}

impl Default for StorageTier {
    fn default() -> Self {
        StorageTier::Hot
    }
}

#[derive(Debug, Clone)]
pub struct TieredStorageConfig {
    pub hot_threshold_bytes: u64,
    pub cold_after_days: u32,
    pub hot_path: String,
    pub cold_path: String,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            hot_threshold_bytes: 10 * 1024 * 1024 * 1024,
            cold_after_days: 30,
            hot_path: "hot".to_string(),
            cold_path: "cold".to_string(),
        }
    }
}

pub struct TieredStorage {
    config: TieredStorageConfig,
    hot_bytes: AtomicU64,
    cold_bytes: AtomicU64,
}

impl TieredStorage {
    pub fn new(config: TieredStorageConfig) -> Self {
        Self {
            config,
            hot_bytes: AtomicU64::new(0),
            cold_bytes: AtomicU64::new(0),
        }
    }

    pub fn determine_tier(&self, size: u64, last_access: Option<SystemTime>) -> StorageTier {
        let hot_usage = self.hot_bytes.load(Ordering::Relaxed);

        if hot_usage + size > self.config.hot_threshold_bytes {
            return StorageTier::Cold;
        }

        if let Some(access_time) = last_access {
            if let Ok(elapsed) = access_time.elapsed() {
                let cold_threshold = Duration::from_secs(self.config.cold_after_days as u64 * 86400);
                if elapsed > cold_threshold {
                    return StorageTier::Cold;
                }
            }
        }

        StorageTier::Hot
    }

    pub fn tier_path(&self, tier: StorageTier, base_path: &Path) -> std::path::PathBuf {
        match tier {
            StorageTier::Hot => base_path.join(&self.config.hot_path),
            StorageTier::Cold => base_path.join(&self.config.cold_path),
        }
    }

    pub fn add_to_tier(&self, tier: StorageTier, bytes: u64) {
        match tier {
            StorageTier::Hot => {
                self.hot_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            StorageTier::Cold => {
                self.cold_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn remove_from_tier(&self, tier: StorageTier, bytes: u64) {
        match tier {
            StorageTier::Hot => {
                self.hot_bytes.fetch_sub(bytes, Ordering::Relaxed);
            }
            StorageTier::Cold => {
                self.cold_bytes.fetch_sub(bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn move_to_cold(&self, bytes: u64) -> Result<()> {
        self.hot_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.cold_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    pub fn promote_to_hot(&self, bytes: u64) -> Result<()> {
        self.cold_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.hot_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    pub fn hot_bytes(&self) -> u64 {
        self.hot_bytes.load(Ordering::Relaxed)
    }

    pub fn cold_bytes(&self) -> u64 {
        self.cold_bytes.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.hot_bytes() + self.cold_bytes()
    }
}

impl Default for TieredStorage {
    fn default() -> Self {
        Self::new(TieredStorageConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_tier_default() {
        let tier = StorageTier::default();
        assert_eq!(tier, StorageTier::Hot);
    }

    #[test]
    fn test_tiered_storage_config_default() {
        let config = TieredStorageConfig::default();
        assert_eq!(config.hot_threshold_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(config.cold_after_days, 30);
    }

    #[test]
    fn test_determine_tier_small_object() {
        let storage = TieredStorage::default();
        let tier = storage.determine_tier(1024, None);
        assert_eq!(tier, StorageTier::Hot);
    }

    #[test]
    fn test_determine_tier_exceeds_threshold() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 1000,
            cold_after_days: 30,
            hot_path: "hot".to_string(),
            cold_path: "cold".to_string(),
        };
        let storage = TieredStorage::new(config);
        storage.add_to_tier(StorageTier::Hot, 900);
        let tier = storage.determine_tier(200, None);
        assert_eq!(tier, StorageTier::Cold);
    }

    #[test]
    fn test_tier_path() {
        let storage = TieredStorage::default();
        let base = Path::new("/data");

        let hot_path = storage.tier_path(StorageTier::Hot, base);
        assert_eq!(hot_path, Path::new("/data/hot"));

        let cold_path = storage.tier_path(StorageTier::Cold, base);
        assert_eq!(cold_path, Path::new("/data/cold"));
    }

    #[test]
    fn test_add_remove_tier() {
        let storage = TieredStorage::default();

        storage.add_to_tier(StorageTier::Hot, 1000);
        assert_eq!(storage.hot_bytes(), 1000);

        storage.remove_from_tier(StorageTier::Hot, 500);
        assert_eq!(storage.hot_bytes(), 500);

        storage.add_to_tier(StorageTier::Cold, 2000);
        assert_eq!(storage.cold_bytes(), 2000);
    }

    #[test]
    fn test_move_between_tiers() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Hot, 1000);

        storage.move_to_cold(500).unwrap();
        assert_eq!(storage.hot_bytes(), 500);
        assert_eq!(storage.cold_bytes(), 500);

        storage.promote_to_hot(200).unwrap();
        assert_eq!(storage.hot_bytes(), 700);
        assert_eq!(storage.cold_bytes(), 300);
    }

    #[test]
    fn test_total_bytes() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Hot, 1000);
        storage.add_to_tier(StorageTier::Cold, 2000);
        assert_eq!(storage.total_bytes(), 3000);
    }

    #[test]
    fn test_tiered_storage_default() {
        let storage = TieredStorage::default();
        assert_eq!(storage.hot_bytes(), 0);
        assert_eq!(storage.cold_bytes(), 0);
    }
}
