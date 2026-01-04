use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageTier {
    #[default]
    Hot,
    Cold,
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
                let cold_threshold =
                    Duration::from_secs(self.config.cold_after_days as u64 * 86400);
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

    #[test]
    fn test_determine_tier_old_access_time_moves_to_cold() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 10 * 1024 * 1024 * 1024,
            cold_after_days: 1,
            hot_path: "hot".to_string(),
            cold_path: "cold".to_string(),
        };
        let storage = TieredStorage::new(config);
        let old_time = SystemTime::now() - Duration::from_secs(2 * 86400);
        let tier = storage.determine_tier(1024, Some(old_time));
        assert_eq!(tier, StorageTier::Cold);
    }

    #[test]
    fn test_determine_tier_recent_access_stays_hot() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 10 * 1024 * 1024 * 1024,
            cold_after_days: 30,
            hot_path: "hot".to_string(),
            cold_path: "cold".to_string(),
        };
        let storage = TieredStorage::new(config);
        let recent_time = SystemTime::now() - Duration::from_secs(86400);
        let tier = storage.determine_tier(1024, Some(recent_time));
        assert_eq!(tier, StorageTier::Hot);
    }

    #[test]
    fn test_remove_from_cold_tier() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Cold, 5000);
        assert_eq!(storage.cold_bytes(), 5000);
        storage.remove_from_tier(StorageTier::Cold, 2000);
        assert_eq!(storage.cold_bytes(), 3000);
    }

    #[test]
    fn test_storage_tier_equality() {
        assert_eq!(StorageTier::Hot, StorageTier::Hot);
        assert_eq!(StorageTier::Cold, StorageTier::Cold);
        assert_ne!(StorageTier::Hot, StorageTier::Cold);
    }

    #[test]
    fn test_tiered_storage_config_custom() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 5 * 1024 * 1024,
            cold_after_days: 7,
            hot_path: "custom_hot".to_string(),
            cold_path: "custom_cold".to_string(),
        };
        assert_eq!(config.hot_threshold_bytes, 5 * 1024 * 1024);
        assert_eq!(config.cold_after_days, 7);
        assert_eq!(config.hot_path, "custom_hot");
        assert_eq!(config.cold_path, "custom_cold");
    }

    #[test]
    fn test_tier_path_with_custom_paths() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 1024,
            cold_after_days: 1,
            hot_path: "fast_storage".to_string(),
            cold_path: "archive".to_string(),
        };
        let storage = TieredStorage::new(config);
        let base = Path::new("/mnt/data");
        assert_eq!(
            storage.tier_path(StorageTier::Hot, base),
            Path::new("/mnt/data/fast_storage")
        );
        assert_eq!(
            storage.tier_path(StorageTier::Cold, base),
            Path::new("/mnt/data/archive")
        );
    }

    #[test]
    fn test_multiple_add_remove_operations() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Hot, 100);
        storage.add_to_tier(StorageTier::Hot, 200);
        storage.add_to_tier(StorageTier::Cold, 300);
        storage.add_to_tier(StorageTier::Cold, 400);
        assert_eq!(storage.hot_bytes(), 300);
        assert_eq!(storage.cold_bytes(), 700);
        assert_eq!(storage.total_bytes(), 1000);

        storage.remove_from_tier(StorageTier::Hot, 50);
        storage.remove_from_tier(StorageTier::Cold, 100);
        assert_eq!(storage.hot_bytes(), 250);
        assert_eq!(storage.cold_bytes(), 600);
        assert_eq!(storage.total_bytes(), 850);
    }

    #[test]
    fn test_move_to_cold_updates_both_tiers() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Hot, 1000);
        assert_eq!(storage.hot_bytes(), 1000);
        assert_eq!(storage.cold_bytes(), 0);

        storage.move_to_cold(400).unwrap();
        assert_eq!(storage.hot_bytes(), 600);
        assert_eq!(storage.cold_bytes(), 400);
        assert_eq!(storage.total_bytes(), 1000);
    }

    #[test]
    fn test_promote_to_hot_updates_both_tiers() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Cold, 1000);
        assert_eq!(storage.cold_bytes(), 1000);
        assert_eq!(storage.hot_bytes(), 0);

        storage.promote_to_hot(600).unwrap();
        assert_eq!(storage.cold_bytes(), 400);
        assert_eq!(storage.hot_bytes(), 600);
        assert_eq!(storage.total_bytes(), 1000);
    }

    #[test]
    fn test_determine_tier_boundary_condition() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 1000,
            cold_after_days: 30,
            hot_path: "hot".to_string(),
            cold_path: "cold".to_string(),
        };
        let storage = TieredStorage::new(config);
        storage.add_to_tier(StorageTier::Hot, 500);
        let tier = storage.determine_tier(500, None);
        assert_eq!(tier, StorageTier::Hot);
        let tier_over = storage.determine_tier(501, None);
        assert_eq!(tier_over, StorageTier::Cold);
    }

    #[test]
    fn test_config_clone() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 2048,
            cold_after_days: 14,
            hot_path: "h".to_string(),
            cold_path: "c".to_string(),
        };
        let cloned = config.clone();
        assert_eq!(cloned.hot_threshold_bytes, config.hot_threshold_bytes);
        assert_eq!(cloned.cold_after_days, config.cold_after_days);
        assert_eq!(cloned.hot_path, config.hot_path);
        assert_eq!(cloned.cold_path, config.cold_path);
    }

    #[test]
    fn test_storage_tier_debug() {
        let hot = StorageTier::Hot;
        let cold = StorageTier::Cold;
        assert_eq!(format!("{:?}", hot), "Hot");
        assert_eq!(format!("{:?}", cold), "Cold");
    }

    #[test]
    fn test_storage_tier_copy() {
        let tier = StorageTier::Hot;
        let copied = tier;
        assert_eq!(tier, copied);
    }

    #[test]
    fn test_tiered_storage_config_debug() {
        let config = TieredStorageConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TieredStorageConfig"));
        assert!(debug_str.contains("hot_threshold_bytes"));
    }

    #[test]
    fn test_concurrent_tier_operations() {
        use std::sync::Arc;
        use std::thread;

        let storage = Arc::new(TieredStorage::default());
        let mut handles = vec![];

        for _ in 0..4 {
            let s = Arc::clone(&storage);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    s.add_to_tier(StorageTier::Hot, 10);
                }
            }));
        }

        for _ in 0..4 {
            let s = Arc::clone(&storage);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    s.add_to_tier(StorageTier::Cold, 20);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(storage.hot_bytes(), 4000);
        assert_eq!(storage.cold_bytes(), 8000);
    }

    #[test]
    fn test_zero_size_operations() {
        let storage = TieredStorage::default();
        storage.add_to_tier(StorageTier::Hot, 0);
        storage.add_to_tier(StorageTier::Cold, 0);
        assert_eq!(storage.hot_bytes(), 0);
        assert_eq!(storage.cold_bytes(), 0);

        storage.add_to_tier(StorageTier::Hot, 100);
        storage.remove_from_tier(StorageTier::Hot, 0);
        assert_eq!(storage.hot_bytes(), 100);

        storage.move_to_cold(0).unwrap();
        assert_eq!(storage.hot_bytes(), 100);
        assert_eq!(storage.cold_bytes(), 0);

        storage.promote_to_hot(0).unwrap();
        assert_eq!(storage.hot_bytes(), 100);
        assert_eq!(storage.cold_bytes(), 0);
    }

    #[test]
    fn test_determine_tier_with_none_access_time_small_object() {
        let storage = TieredStorage::default();
        let tier = storage.determine_tier(100, None);
        assert_eq!(tier, StorageTier::Hot);
    }

    #[test]
    fn test_full_tier_transition_workflow() {
        let config = TieredStorageConfig {
            hot_threshold_bytes: 5000,
            cold_after_days: 7,
            hot_path: "hot".to_string(),
            cold_path: "cold".to_string(),
        };
        let storage = TieredStorage::new(config);

        storage.add_to_tier(StorageTier::Hot, 2000);
        storage.add_to_tier(StorageTier::Hot, 1500);
        assert_eq!(storage.hot_bytes(), 3500);

        let tier = storage.determine_tier(1000, None);
        assert_eq!(tier, StorageTier::Hot);

        let tier = storage.determine_tier(2000, None);
        assert_eq!(tier, StorageTier::Cold);

        storage.move_to_cold(1500).unwrap();
        assert_eq!(storage.hot_bytes(), 2000);
        assert_eq!(storage.cold_bytes(), 1500);

        let tier = storage.determine_tier(2000, None);
        assert_eq!(tier, StorageTier::Hot);

        storage.promote_to_hot(500).unwrap();
        assert_eq!(storage.hot_bytes(), 2500);
        assert_eq!(storage.cold_bytes(), 1000);
    }

    #[test]
    fn test_tier_path_empty_base() {
        let storage = TieredStorage::default();
        let base = Path::new("");
        assert_eq!(storage.tier_path(StorageTier::Hot, base), Path::new("hot"));
        assert_eq!(
            storage.tier_path(StorageTier::Cold, base),
            Path::new("cold")
        );
    }

    #[test]
    fn test_large_byte_values() {
        let storage = TieredStorage::default();
        let large_value = u64::MAX / 2;
        storage.add_to_tier(StorageTier::Hot, large_value);
        assert_eq!(storage.hot_bytes(), large_value);
        storage.add_to_tier(StorageTier::Cold, large_value);
        assert_eq!(storage.cold_bytes(), large_value);
        assert_eq!(storage.total_bytes(), large_value * 2);
    }

    #[test]
    fn test_determine_tier_future_access_time_stays_hot() {
        let storage = TieredStorage::default();
        let future_time = SystemTime::now() + Duration::from_secs(3600);
        let tier = storage.determine_tier(1024, Some(future_time));
        assert_eq!(tier, StorageTier::Hot);
    }
}
