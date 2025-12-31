use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcState {
    Idle,
    Marking,
    Sweeping,
    Paused,
}

impl Default for GcState {
    fn default() -> Self {
        GcState::Idle
    }
}

pub struct GcSchedulerConfig {
    pub interval: Duration,
    pub min_objects_threshold: u64,
    pub max_pause_ms: u64,
    pub enabled: bool,
}

impl Default for GcSchedulerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3600),
            min_objects_threshold: 1000,
            max_pause_ms: 100,
            enabled: true,
        }
    }
}

pub struct GcScheduler {
    config: GcSchedulerConfig,
    state: RwLock<GcState>,
    last_run: RwLock<Option<Instant>>,
    run_count: AtomicU64,
    total_duration_ms: AtomicU64,
    objects_collected: AtomicU64,
    bytes_freed: AtomicU64,
    is_running: AtomicBool,
}

impl GcScheduler {
    pub fn new(config: GcSchedulerConfig) -> Self {
        Self {
            config,
            state: RwLock::new(GcState::Idle),
            last_run: RwLock::new(None),
            run_count: AtomicU64::new(0),
            total_duration_ms: AtomicU64::new(0),
            objects_collected: AtomicU64::new(0),
            bytes_freed: AtomicU64::new(0),
            is_running: AtomicBool::new(false),
        }
    }

    pub fn should_run(&self, object_count: u64) -> bool {
        if !self.config.enabled {
            return false;
        }

        if self.is_running.load(Ordering::Relaxed) {
            return false;
        }

        if object_count < self.config.min_objects_threshold {
            return false;
        }

        let last_run = self.last_run.read();
        match *last_run {
            Some(last) => last.elapsed() >= self.config.interval,
            None => true,
        }
    }

    pub fn start(&self) -> bool {
        if self
            .is_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            *self.state.write() = GcState::Marking;
            true
        } else {
            false
        }
    }

    pub fn transition_to_sweep(&self) {
        *self.state.write() = GcState::Sweeping;
    }

    pub fn pause(&self) {
        *self.state.write() = GcState::Paused;
    }

    pub fn resume(&self) {
        let current = *self.state.read();
        if current == GcState::Paused {
            *self.state.write() = GcState::Sweeping;
        }
    }

    pub fn complete(&self, objects_collected: u64, bytes_freed: u64, duration: Duration) {
        *self.state.write() = GcState::Idle;
        *self.last_run.write() = Some(Instant::now());
        self.is_running.store(false, Ordering::Relaxed);

        self.run_count.fetch_add(1, Ordering::Relaxed);
        self.total_duration_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
        self.objects_collected
            .fetch_add(objects_collected, Ordering::Relaxed);
        self.bytes_freed.fetch_add(bytes_freed, Ordering::Relaxed);
    }

    pub fn abort(&self) {
        *self.state.write() = GcState::Idle;
        self.is_running.store(false, Ordering::Relaxed);
    }

    pub fn state(&self) -> GcState {
        *self.state.read()
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    pub fn time_until_next(&self) -> Option<Duration> {
        if !self.config.enabled {
            return None;
        }

        let last_run = self.last_run.read();
        match *last_run {
            Some(last) => {
                let elapsed = last.elapsed();
                if elapsed >= self.config.interval {
                    Some(Duration::ZERO)
                } else {
                    Some(self.config.interval - elapsed)
                }
            }
            None => Some(Duration::ZERO),
        }
    }

    pub fn stats(&self) -> GcSchedulerStats {
        GcSchedulerStats {
            state: self.state(),
            run_count: self.run_count.load(Ordering::Relaxed),
            total_duration_ms: self.total_duration_ms.load(Ordering::Relaxed),
            objects_collected: self.objects_collected.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
            is_enabled: self.config.enabled,
        }
    }
}

impl Default for GcScheduler {
    fn default() -> Self {
        Self::new(GcSchedulerConfig::default())
    }
}

#[derive(Debug, Clone)]
pub struct GcSchedulerStats {
    pub state: GcState,
    pub run_count: u64,
    pub total_duration_ms: u64,
    pub objects_collected: u64,
    pub bytes_freed: u64,
    pub is_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_state_default() {
        let state = GcState::default();
        assert_eq!(state, GcState::Idle);
    }

    #[test]
    fn test_gc_scheduler_config_default() {
        let config = GcSchedulerConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3600));
        assert_eq!(config.min_objects_threshold, 1000);
        assert!(config.enabled);
    }

    #[test]
    fn test_scheduler_new() {
        let scheduler = GcScheduler::default();
        assert_eq!(scheduler.state(), GcState::Idle);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_should_run_disabled() {
        let config = GcSchedulerConfig {
            enabled: false,
            ..Default::default()
        };
        let scheduler = GcScheduler::new(config);
        assert!(!scheduler.should_run(10000));
    }

    #[test]
    fn test_should_run_below_threshold() {
        let scheduler = GcScheduler::default();
        assert!(!scheduler.should_run(500));
    }

    #[test]
    fn test_should_run_first_time() {
        let scheduler = GcScheduler::default();
        assert!(scheduler.should_run(5000));
    }

    #[test]
    fn test_start_gc() {
        let scheduler = GcScheduler::default();

        assert!(scheduler.start());
        assert!(scheduler.is_running());
        assert_eq!(scheduler.state(), GcState::Marking);
    }

    #[test]
    fn test_start_gc_already_running() {
        let scheduler = GcScheduler::default();

        assert!(scheduler.start());
        assert!(!scheduler.start());
    }

    #[test]
    fn test_transition_to_sweep() {
        let scheduler = GcScheduler::default();
        scheduler.start();
        scheduler.transition_to_sweep();
        assert_eq!(scheduler.state(), GcState::Sweeping);
    }

    #[test]
    fn test_pause_resume() {
        let scheduler = GcScheduler::default();
        scheduler.start();
        scheduler.transition_to_sweep();

        scheduler.pause();
        assert_eq!(scheduler.state(), GcState::Paused);

        scheduler.resume();
        assert_eq!(scheduler.state(), GcState::Sweeping);
    }

    #[test]
    fn test_complete() {
        let scheduler = GcScheduler::default();
        scheduler.start();

        scheduler.complete(100, 10240, Duration::from_millis(500));

        assert_eq!(scheduler.state(), GcState::Idle);
        assert!(!scheduler.is_running());

        let stats = scheduler.stats();
        assert_eq!(stats.run_count, 1);
        assert_eq!(stats.objects_collected, 100);
        assert_eq!(stats.bytes_freed, 10240);
    }

    #[test]
    fn test_abort() {
        let scheduler = GcScheduler::default();
        scheduler.start();
        scheduler.abort();

        assert_eq!(scheduler.state(), GcState::Idle);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_set_enabled() {
        let mut scheduler = GcScheduler::default();
        assert!(scheduler.is_enabled());

        scheduler.set_enabled(false);
        assert!(!scheduler.is_enabled());
    }

    #[test]
    fn test_time_until_next_disabled() {
        let config = GcSchedulerConfig {
            enabled: false,
            ..Default::default()
        };
        let scheduler = GcScheduler::new(config);
        assert!(scheduler.time_until_next().is_none());
    }

    #[test]
    fn test_time_until_next_never_run() {
        let scheduler = GcScheduler::default();
        let time = scheduler.time_until_next();
        assert_eq!(time, Some(Duration::ZERO));
    }

    #[test]
    fn test_time_until_next_after_run() {
        let scheduler = GcScheduler::default();
        scheduler.start();
        scheduler.complete(0, 0, Duration::ZERO);

        let time = scheduler.time_until_next();
        assert!(time.is_some());
        assert!(time.unwrap() > Duration::ZERO);
    }

    #[test]
    fn test_scheduler_default() {
        let scheduler = GcScheduler::default();
        assert!(scheduler.is_enabled());
    }
}
