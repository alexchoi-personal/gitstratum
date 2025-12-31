use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub struct MetricsMiddleware {
    requests_total: AtomicU64,
    requests_success: AtomicU64,
    requests_error: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    clone_count: AtomicU64,
    fetch_count: AtomicU64,
    push_count: AtomicU64,
}

impl MetricsMiddleware {
    pub fn new() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            requests_success: AtomicU64::new(0),
            requests_error: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            clone_count: AtomicU64::new(0),
            fetch_count: AtomicU64::new(0),
            push_count: AtomicU64::new(0),
        }
    }

    pub fn record_request_start(&self) -> RequestTimer {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        RequestTimer {
            start: Instant::now(),
        }
    }

    pub fn record_request_success(&self) {
        self.requests_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_request_error(&self) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_clone(&self) {
        self.clone_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fetch(&self) {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_push(&self) {
        self.push_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn requests_total(&self) -> u64 {
        self.requests_total.load(Ordering::Relaxed)
    }

    pub fn requests_success(&self) -> u64 {
        self.requests_success.load(Ordering::Relaxed)
    }

    pub fn requests_error(&self) -> u64 {
        self.requests_error.load(Ordering::Relaxed)
    }

    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    pub fn clone_count(&self) -> u64 {
        self.clone_count.load(Ordering::Relaxed)
    }

    pub fn fetch_count(&self) -> u64 {
        self.fetch_count.load(Ordering::Relaxed)
    }

    pub fn push_count(&self) -> u64 {
        self.push_count.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            requests_total: self.requests_total(),
            requests_success: self.requests_success(),
            requests_error: self.requests_error(),
            bytes_sent: self.bytes_sent(),
            bytes_received: self.bytes_received(),
            clone_count: self.clone_count(),
            fetch_count: self.fetch_count(),
            push_count: self.push_count(),
        }
    }
}

impl Default for MetricsMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub requests_total: u64,
    pub requests_success: u64,
    pub requests_error: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub clone_count: u64,
    pub fetch_count: u64,
    pub push_count: u64,
}

pub struct RequestTimer {
    start: Instant,
}

impl RequestTimer {
    pub fn elapsed_millis(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    pub fn elapsed_micros(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_middleware_new() {
        let metrics = MetricsMiddleware::new();
        assert_eq!(metrics.requests_total(), 0);
        assert_eq!(metrics.requests_success(), 0);
        assert_eq!(metrics.requests_error(), 0);
    }

    #[test]
    fn test_metrics_middleware_default() {
        let metrics = MetricsMiddleware::default();
        assert_eq!(metrics.requests_total(), 0);
    }

    #[test]
    fn test_record_request() {
        let metrics = MetricsMiddleware::new();

        let _timer = metrics.record_request_start();
        assert_eq!(metrics.requests_total(), 1);

        metrics.record_request_success();
        assert_eq!(metrics.requests_success(), 1);

        metrics.record_request_error();
        assert_eq!(metrics.requests_error(), 1);
    }

    #[test]
    fn test_record_bytes() {
        let metrics = MetricsMiddleware::new();

        metrics.record_bytes_sent(1000);
        metrics.record_bytes_received(500);

        assert_eq!(metrics.bytes_sent(), 1000);
        assert_eq!(metrics.bytes_received(), 500);
    }

    #[test]
    fn test_record_operations() {
        let metrics = MetricsMiddleware::new();

        metrics.record_clone();
        metrics.record_fetch();
        metrics.record_push();

        assert_eq!(metrics.clone_count(), 1);
        assert_eq!(metrics.fetch_count(), 1);
        assert_eq!(metrics.push_count(), 1);
    }

    #[test]
    fn test_snapshot() {
        let metrics = MetricsMiddleware::new();

        metrics.record_request_start();
        metrics.record_request_success();
        metrics.record_bytes_sent(100);
        metrics.record_clone();

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.requests_success, 1);
        assert_eq!(snapshot.bytes_sent, 100);
        assert_eq!(snapshot.clone_count, 1);
    }

    #[test]
    fn test_request_timer() {
        let metrics = MetricsMiddleware::new();
        let timer = metrics.record_request_start();

        std::thread::sleep(std::time::Duration::from_millis(1));

        assert!(timer.elapsed_millis() >= 1);
        assert!(timer.elapsed_micros() >= 1000);
    }
}
