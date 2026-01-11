use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

pub struct RetryPolicy {
    max_attempts: usize,
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl RetryPolicy {
    pub fn new(max_attempts: usize, initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay,
            multiplier: 2.0,
        }
    }

    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub async fn execute<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut delay = self.initial_delay;
        let mut last_err = None;

        for _ in 0..self.max_attempts {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => last_err = Some(e),
            }
            sleep(delay).await;
            delay = Duration::from_secs_f64(
                (delay.as_secs_f64() * self.multiplier).min(self.max_delay.as_secs_f64()),
            );
        }

        Err(last_err.unwrap())
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3, Duration::from_millis(100), Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_policy_new() {
        let policy = RetryPolicy::new(5, Duration::from_millis(50), Duration::from_secs(10));
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_delay, Duration::from_millis(50));
        assert_eq!(policy.max_delay, Duration::from_secs(10));
        assert!((policy.multiplier - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_delay, Duration::from_millis(100));
        assert_eq!(policy.max_delay, Duration::from_secs(5));
        assert!((policy.multiplier - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_policy_with_multiplier() {
        let policy = RetryPolicy::default().with_multiplier(1.5);
        assert!((policy.multiplier - 1.5).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_execute_success_first_try() {
        let policy = RetryPolicy::default();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<i32, &str> = policy
            .execute(|| {
                let c = Arc::clone(&counter_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_execute_success_after_retries() {
        let policy = RetryPolicy::new(5, Duration::from_millis(1), Duration::from_millis(10));
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<i32, &str> = policy
            .execute(|| {
                let c = Arc::clone(&counter_clone);
                async move {
                    let count = c.fetch_add(1, Ordering::SeqCst);
                    if count < 2 {
                        Err("not yet")
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_execute_all_retries_fail() {
        let policy = RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10));
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<i32, &str> = policy
            .execute(|| {
                let c = Arc::clone(&counter_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err("always fails")
                }
            })
            .await;

        assert_eq!(result.unwrap_err(), "always fails");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_execute_delay_capped_at_max() {
        let policy = RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(150))
            .with_multiplier(10.0);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<i32, &str> = policy
            .execute(|| {
                let c = Arc::clone(&counter_clone);
                async move {
                    let count = c.fetch_add(1, Ordering::SeqCst);
                    if count < 4 {
                        Err("not yet")
                    } else {
                        Ok(100)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 100);
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_execute_single_attempt() {
        let policy = RetryPolicy::new(1, Duration::from_millis(1), Duration::from_millis(10));
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<i32, &str> = policy
            .execute(|| {
                let c = Arc::clone(&counter_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err("fails")
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_execute_with_custom_error_type() {
        #[derive(Debug, PartialEq)]
        struct CustomError(String);

        let policy = RetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(10));
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let result: Result<i32, CustomError> = policy
            .execute(|| {
                let c = Arc::clone(&counter_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err(CustomError("custom error".to_string()))
                }
            })
            .await;

        assert_eq!(result.unwrap_err(), CustomError("custom error".to_string()));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
