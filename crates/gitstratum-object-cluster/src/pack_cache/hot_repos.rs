use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
pub struct HotRepoConfig {
    pub window_duration: Duration,
    pub default_threshold: u64,
    pub max_hot_repos: usize,
}

impl Default for HotRepoConfig {
    fn default() -> Self {
        Self {
            window_duration: Duration::from_secs(300),
            default_threshold: 10,
            max_hot_repos: 100,
        }
    }
}

pub struct HotRepoTracker {
    access_counts: DashMap<String, AtomicU64>,
    window_start: RwLock<Instant>,
    window_duration: Duration,
    config: HotRepoConfig,
}

impl HotRepoTracker {
    pub fn new(window_duration: Duration) -> Self {
        Self::with_config(HotRepoConfig {
            window_duration,
            ..Default::default()
        })
    }

    pub fn with_config(config: HotRepoConfig) -> Self {
        Self {
            access_counts: DashMap::new(),
            window_start: RwLock::new(Instant::now()),
            window_duration: config.window_duration,
            config,
        }
    }

    #[instrument(skip(self))]
    pub fn record_access(&self, repo_id: &str) {
        self.maybe_reset_window();

        if let Some(counter) = self.access_counts.get(repo_id) {
            counter.fetch_add(1, Ordering::Relaxed);
        } else {
            self.access_counts
                .entry(repo_id.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(1, Ordering::Relaxed);
        }

        debug!(repo_id = %repo_id, "recorded repo access");
    }

    pub fn get_access_count(&self, repo_id: &str) -> u64 {
        self.access_counts
            .get(repo_id)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    #[instrument(skip(self))]
    pub fn get_hot_repos(&self, threshold: u64) -> Vec<String> {
        self.maybe_reset_window();

        let max_repos = self.config.max_hot_repos;
        let mut heap: BinaryHeap<Reverse<(u64, String)>> = BinaryHeap::with_capacity(max_repos + 1);

        for entry in self.access_counts.iter() {
            let count = entry.value().load(Ordering::Relaxed);
            if count >= threshold {
                if heap.len() < max_repos {
                    heap.push(Reverse((count, entry.key().clone())));
                } else if let Some(&Reverse((min_count, _))) = heap.peek() {
                    if count > min_count {
                        heap.pop();
                        heap.push(Reverse((count, entry.key().clone())));
                    }
                }
            }
        }

        let mut repos: Vec<(u64, String)> = heap.into_iter().map(|Reverse(item)| item).collect();
        repos.sort_by(|a, b| b.0.cmp(&a.0));

        let repos: Vec<String> = repos.into_iter().map(|(_, repo)| repo).collect();

        debug!(
            count = repos.len(),
            threshold = threshold,
            "retrieved hot repos"
        );

        repos
    }

    pub fn get_hot_repos_default(&self) -> Vec<String> {
        self.get_hot_repos(self.config.default_threshold)
    }

    pub fn get_top_repos(&self, limit: usize) -> Vec<(String, u64)> {
        self.maybe_reset_window();

        let mut repos: Vec<(String, u64)> = self
            .access_counts
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();

        repos.sort_by(|a, b| b.1.cmp(&a.1));
        repos.truncate(limit);
        repos
    }

    #[instrument(skip(self))]
    pub fn reset_window(&self) {
        let mut window_start = self.window_start.write();
        *window_start = Instant::now();
        self.access_counts.clear();
        debug!("reset tracking window");
    }

    fn maybe_reset_window(&self) {
        let should_reset = {
            let window_start = self.window_start.read();
            window_start.elapsed() > self.window_duration
        };

        if should_reset {
            self.reset_window();
        }
    }

    pub fn window_elapsed(&self) -> Duration {
        self.window_start.read().elapsed()
    }

    pub fn window_remaining(&self) -> Duration {
        let elapsed = self.window_elapsed();
        if elapsed >= self.window_duration {
            Duration::ZERO
        } else {
            self.window_duration - elapsed
        }
    }

    pub fn repo_count(&self) -> usize {
        self.access_counts.len()
    }

    pub fn total_accesses(&self) -> u64 {
        self.access_counts
            .iter()
            .map(|entry| entry.value().load(Ordering::Relaxed))
            .sum()
    }

    pub fn stats(&self) -> HotRepoStats {
        let threshold = self.config.default_threshold;
        let (repo_count, hot_count, total_accesses) = self.access_counts.iter().fold(
            (0usize, 0usize, 0u64),
            |(repo_count, hot_count, total), entry| {
                let count = entry.value().load(Ordering::Relaxed);
                (
                    repo_count + 1,
                    hot_count + (count >= threshold) as usize,
                    total + count,
                )
            },
        );

        HotRepoStats {
            repo_count,
            hot_count,
            total_accesses,
            window_elapsed: self.window_elapsed(),
            window_duration: self.window_duration,
        }
    }

    pub fn remove_repo(&self, repo_id: &str) {
        self.access_counts.remove(repo_id);
    }

    pub fn contains(&self, repo_id: &str) -> bool {
        self.access_counts.contains_key(repo_id)
    }
}

#[derive(Debug, Clone)]
pub struct HotRepoStats {
    pub repo_count: usize,
    pub hot_count: usize,
    pub total_accesses: u64,
    pub window_elapsed: Duration,
    pub window_duration: Duration,
}

impl HotRepoStats {
    pub fn window_progress(&self) -> f64 {
        if self.window_duration.is_zero() {
            return 1.0;
        }
        self.window_elapsed.as_secs_f64() / self.window_duration.as_secs_f64()
    }

    pub fn hot_ratio(&self) -> f64 {
        if self.repo_count == 0 {
            return 0.0;
        }
        self.hot_count as f64 / self.repo_count as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_repo_config_default() {
        let config = HotRepoConfig::default();
        assert_eq!(config.window_duration, Duration::from_secs(300));
        assert_eq!(config.default_threshold, 10);
        assert_eq!(config.max_hot_repos, 100);
    }

    #[test]
    fn test_hot_repo_tracker_new() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));
        assert_eq!(tracker.repo_count(), 0);
        assert_eq!(tracker.total_accesses(), 0);
    }

    #[test]
    fn test_hot_repo_tracker_with_config() {
        let config = HotRepoConfig {
            window_duration: Duration::from_secs(120),
            default_threshold: 5,
            max_hot_repos: 50,
        };
        let tracker = HotRepoTracker::with_config(config);
        assert_eq!(tracker.config.default_threshold, 5);
        assert_eq!(tracker.config.max_hot_repos, 50);
    }

    #[test]
    fn test_record_access() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));

        tracker.record_access("repo1");
        assert_eq!(tracker.get_access_count("repo1"), 1);

        tracker.record_access("repo1");
        assert_eq!(tracker.get_access_count("repo1"), 2);

        tracker.record_access("repo2");
        assert_eq!(tracker.get_access_count("repo2"), 1);

        assert_eq!(tracker.repo_count(), 2);
        assert_eq!(tracker.total_accesses(), 3);
    }

    #[test]
    fn test_get_access_count_nonexistent() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));
        assert_eq!(tracker.get_access_count("nonexistent"), 0);
    }

    #[test]
    fn test_get_hot_repos() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));

        for _ in 0..15 {
            tracker.record_access("hot-repo");
        }
        for _ in 0..5 {
            tracker.record_access("warm-repo");
        }
        for _ in 0..2 {
            tracker.record_access("cold-repo");
        }

        let hot = tracker.get_hot_repos(10);
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0], "hot-repo");

        let warm = tracker.get_hot_repos(5);
        assert_eq!(warm.len(), 2);
        assert!(warm.contains(&"hot-repo".to_string()));
        assert!(warm.contains(&"warm-repo".to_string()));
    }

    #[test]
    fn test_get_hot_repos_default() {
        let config = HotRepoConfig {
            default_threshold: 3,
            ..Default::default()
        };
        let tracker = HotRepoTracker::with_config(config);

        for _ in 0..5 {
            tracker.record_access("hot-repo");
        }
        for _ in 0..1 {
            tracker.record_access("cold-repo");
        }

        let hot = tracker.get_hot_repos_default();
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0], "hot-repo");
    }

    #[test]
    fn test_get_hot_repos_max_limit() {
        let config = HotRepoConfig {
            max_hot_repos: 2,
            default_threshold: 1,
            ..Default::default()
        };
        let tracker = HotRepoTracker::with_config(config);

        for i in 0..5 {
            for _ in 0..(5 - i) {
                tracker.record_access(&format!("repo-{}", i));
            }
        }

        let hot = tracker.get_hot_repos(1);
        assert_eq!(hot.len(), 2);
        assert_eq!(hot[0], "repo-0");
        assert_eq!(hot[1], "repo-1");
    }

    #[test]
    fn test_get_top_repos() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));

        for _ in 0..10 {
            tracker.record_access("repo-a");
        }
        for _ in 0..5 {
            tracker.record_access("repo-b");
        }
        for _ in 0..3 {
            tracker.record_access("repo-c");
        }

        let top = tracker.get_top_repos(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "repo-a");
        assert_eq!(top[0].1, 10);
        assert_eq!(top[1].0, "repo-b");
        assert_eq!(top[1].1, 5);
    }

    #[test]
    fn test_reset_window() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));

        tracker.record_access("repo1");
        tracker.record_access("repo2");
        assert_eq!(tracker.repo_count(), 2);

        tracker.reset_window();

        assert_eq!(tracker.repo_count(), 0);
        assert_eq!(tracker.get_access_count("repo1"), 0);
    }

    #[test]
    fn test_auto_reset_window() {
        let tracker = HotRepoTracker::new(Duration::from_millis(10));

        tracker.record_access("repo1");
        assert_eq!(tracker.get_access_count("repo1"), 1);

        std::thread::sleep(Duration::from_millis(20));

        tracker.record_access("repo2");

        assert_eq!(tracker.get_access_count("repo1"), 0);
        assert_eq!(tracker.get_access_count("repo2"), 1);
    }

    #[test]
    fn test_window_elapsed() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(10));
        assert!(tracker.window_elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn test_window_remaining() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));
        let remaining = tracker.window_remaining();
        assert!(remaining <= Duration::from_secs(60));
        assert!(remaining > Duration::from_secs(59));
    }

    #[test]
    fn test_window_remaining_expired() {
        let tracker = HotRepoTracker::new(Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(tracker.window_remaining(), Duration::ZERO);
    }

    #[test]
    fn test_stats() {
        let config = HotRepoConfig {
            default_threshold: 5,
            ..Default::default()
        };
        let tracker = HotRepoTracker::with_config(config);

        for _ in 0..10 {
            tracker.record_access("hot-repo");
        }
        for _ in 0..3 {
            tracker.record_access("cold-repo");
        }

        let stats = tracker.stats();
        assert_eq!(stats.repo_count, 2);
        assert_eq!(stats.hot_count, 1);
        assert_eq!(stats.total_accesses, 13);
    }

    #[test]
    fn test_stats_window_progress() {
        let stats = HotRepoStats {
            repo_count: 10,
            hot_count: 5,
            total_accesses: 100,
            window_elapsed: Duration::from_secs(30),
            window_duration: Duration::from_secs(60),
        };

        assert!((stats.window_progress() - 0.5).abs() < 0.001);

        let zero_duration_stats = HotRepoStats {
            repo_count: 10,
            hot_count: 5,
            total_accesses: 100,
            window_elapsed: Duration::from_secs(30),
            window_duration: Duration::ZERO,
        };
        assert_eq!(zero_duration_stats.window_progress(), 1.0);
    }

    #[test]
    fn test_stats_hot_ratio() {
        let stats = HotRepoStats {
            repo_count: 10,
            hot_count: 4,
            total_accesses: 100,
            window_elapsed: Duration::from_secs(30),
            window_duration: Duration::from_secs(60),
        };

        assert!((stats.hot_ratio() - 0.4).abs() < 0.001);

        let empty_stats = HotRepoStats {
            repo_count: 0,
            hot_count: 0,
            total_accesses: 0,
            window_elapsed: Duration::from_secs(30),
            window_duration: Duration::from_secs(60),
        };
        assert_eq!(empty_stats.hot_ratio(), 0.0);
    }

    #[test]
    fn test_remove_repo() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));

        tracker.record_access("repo1");
        tracker.record_access("repo2");
        assert_eq!(tracker.repo_count(), 2);

        tracker.remove_repo("repo1");
        assert_eq!(tracker.repo_count(), 1);
        assert_eq!(tracker.get_access_count("repo1"), 0);
        assert_eq!(tracker.get_access_count("repo2"), 1);
    }

    #[test]
    fn test_contains() {
        let tracker = HotRepoTracker::new(Duration::from_secs(60));

        assert!(!tracker.contains("repo1"));

        tracker.record_access("repo1");
        assert!(tracker.contains("repo1"));
        assert!(!tracker.contains("repo2"));
    }

    #[test]
    fn test_hot_repo_config_debug() {
        let config = HotRepoConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("HotRepoConfig"));
    }

    #[test]
    fn test_hot_repo_stats_debug() {
        let stats = HotRepoStats {
            repo_count: 10,
            hot_count: 5,
            total_accesses: 100,
            window_elapsed: Duration::from_secs(30),
            window_duration: Duration::from_secs(60),
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("HotRepoStats"));
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(HotRepoTracker::new(Duration::from_secs(60)));

        let mut handles = vec![];

        for i in 0..4 {
            let tracker = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    tracker.record_access(&format!("repo-{}", i % 2));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(tracker.total_accesses(), 400);
    }
}
