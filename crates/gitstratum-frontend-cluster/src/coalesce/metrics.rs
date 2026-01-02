use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct CoalesceMetrics {
    leader_requests: AtomicU64,
    coalesced_requests: AtomicU64,
    total_requests: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceStats {
    pub leader_requests: u64,
    pub coalesced_requests: u64,
    pub total_requests: u64,
}

impl CoalesceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_leader(&self) {
        self.leader_requests.fetch_add(1, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_coalesced(&self) {
        self.coalesced_requests.fetch_add(1, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> CoalesceStats {
        CoalesceStats {
            leader_requests: self.leader_requests.load(Ordering::Relaxed),
            coalesced_requests: self.coalesced_requests.load(Ordering::Relaxed),
            total_requests: self.total_requests.load(Ordering::Relaxed),
        }
    }

    pub fn leader_count(&self) -> u64 {
        self.leader_requests.load(Ordering::Relaxed)
    }

    pub fn coalesced_count(&self) -> u64 {
        self.coalesced_requests.load(Ordering::Relaxed)
    }

    pub fn total_count(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    pub fn coalesce_ratio(&self) -> f64 {
        let total = self.total_count();
        if total == 0 {
            return 0.0;
        }
        self.coalesced_count() as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = CoalesceMetrics::new();
        assert_eq!(metrics.leader_count(), 0);
        assert_eq!(metrics.coalesced_count(), 0);
        assert_eq!(metrics.total_count(), 0);
    }

    #[test]
    fn test_metrics_default() {
        let metrics = CoalesceMetrics::default();
        assert_eq!(metrics.total_count(), 0);
    }

    #[test]
    fn test_inc_leader() {
        let metrics = CoalesceMetrics::new();

        metrics.inc_leader();

        assert_eq!(metrics.leader_count(), 1);
        assert_eq!(metrics.coalesced_count(), 0);
        assert_eq!(metrics.total_count(), 1);
    }

    #[test]
    fn test_inc_coalesced() {
        let metrics = CoalesceMetrics::new();

        metrics.inc_coalesced();

        assert_eq!(metrics.leader_count(), 0);
        assert_eq!(metrics.coalesced_count(), 1);
        assert_eq!(metrics.total_count(), 1);
    }

    #[test]
    fn test_multiple_increments() {
        let metrics = CoalesceMetrics::new();

        metrics.inc_leader();
        metrics.inc_leader();
        metrics.inc_coalesced();
        metrics.inc_coalesced();
        metrics.inc_coalesced();

        assert_eq!(metrics.leader_count(), 2);
        assert_eq!(metrics.coalesced_count(), 3);
        assert_eq!(metrics.total_count(), 5);
    }

    #[test]
    fn test_get_stats() {
        let metrics = CoalesceMetrics::new();

        metrics.inc_leader();
        metrics.inc_leader();
        metrics.inc_coalesced();

        let stats = metrics.get_stats();

        assert_eq!(stats.leader_requests, 2);
        assert_eq!(stats.coalesced_requests, 1);
        assert_eq!(stats.total_requests, 3);
    }

    #[test]
    fn test_coalesce_ratio_empty() {
        let metrics = CoalesceMetrics::new();
        assert_eq!(metrics.coalesce_ratio(), 0.0);
    }

    #[test]
    fn test_coalesce_ratio_all_leaders() {
        let metrics = CoalesceMetrics::new();
        metrics.inc_leader();
        metrics.inc_leader();

        assert_eq!(metrics.coalesce_ratio(), 0.0);
    }

    #[test]
    fn test_coalesce_ratio_mixed() {
        let metrics = CoalesceMetrics::new();
        metrics.inc_leader();
        metrics.inc_coalesced();
        metrics.inc_coalesced();
        metrics.inc_coalesced();

        let ratio = metrics.coalesce_ratio();
        assert!((ratio - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_stats_clone() {
        let metrics = CoalesceMetrics::new();
        metrics.inc_leader();

        let stats1 = metrics.get_stats();
        let stats2 = stats1.clone();

        assert_eq!(stats1, stats2);
    }

    #[test]
    fn test_stats_debug() {
        let metrics = CoalesceMetrics::new();
        metrics.inc_leader();

        let stats = metrics.get_stats();
        let debug = format!("{:?}", stats);

        assert!(debug.contains("CoalesceStats"));
        assert!(debug.contains("leader_requests"));
    }

    #[test]
    fn test_metrics_debug() {
        let metrics = CoalesceMetrics::new();
        let debug = format!("{:?}", metrics);
        assert!(debug.contains("CoalesceMetrics"));
    }

    #[test]
    fn test_concurrent_increments() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(CoalesceMetrics::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    m.inc_leader();
                }
            }));
        }

        for _ in 0..10 {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    m.inc_coalesced();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(metrics.leader_count(), 1000);
        assert_eq!(metrics.coalesced_count(), 1000);
        assert_eq!(metrics.total_count(), 2000);
    }
}
