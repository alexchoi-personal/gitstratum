use std::time::Instant;
use tracing::{debug, error, info, warn, Level};

#[derive(Debug, Clone)]
pub struct LoggingMiddleware {
    level: Level,
    include_timing: bool,
    include_request_id: bool,
}

impl LoggingMiddleware {
    pub fn new() -> Self {
        Self {
            level: Level::INFO,
            include_timing: true,
            include_request_id: true,
        }
    }

    pub fn with_level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    pub fn with_timing(mut self, include: bool) -> Self {
        self.include_timing = include;
        self
    }

    pub fn with_request_id(mut self, include: bool) -> Self {
        self.include_request_id = include;
        self
    }

    pub fn log_request_start(&self, request_id: &str, operation: &str, repo_id: &str) -> RequestLog {
        if self.include_request_id {
            info!(request_id, operation, repo_id, "request started");
        } else {
            info!(operation, repo_id, "request started");
        }

        RequestLog {
            request_id: request_id.to_string(),
            operation: operation.to_string(),
            repo_id: repo_id.to_string(),
            start: Instant::now(),
            include_timing: self.include_timing,
        }
    }

    pub fn log_request_success(&self, log: &RequestLog, bytes: Option<u64>) {
        let elapsed = log.start.elapsed();
        if self.include_timing {
            info!(
                request_id = %log.request_id,
                operation = %log.operation,
                repo_id = %log.repo_id,
                elapsed_ms = elapsed.as_millis() as u64,
                bytes = bytes,
                "request completed successfully"
            );
        } else {
            info!(
                request_id = %log.request_id,
                operation = %log.operation,
                repo_id = %log.repo_id,
                bytes = bytes,
                "request completed successfully"
            );
        }
    }

    pub fn log_request_error(&self, log: &RequestLog, error: &str) {
        let elapsed = log.start.elapsed();
        error!(
            request_id = %log.request_id,
            operation = %log.operation,
            repo_id = %log.repo_id,
            elapsed_ms = elapsed.as_millis() as u64,
            error = error,
            "request failed"
        );
    }

    pub fn log_auth_success(&self, request_id: &str, user_id: &str) {
        debug!(request_id, user_id, "authentication successful");
    }

    pub fn log_auth_failure(&self, request_id: &str, reason: &str) {
        warn!(request_id, reason, "authentication failed");
    }

    pub fn log_rate_limit(&self, request_id: &str, user_id: &str, limit_type: &str) {
        warn!(request_id, user_id, limit_type, "rate limit exceeded");
    }
}

impl Default for LoggingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RequestLog {
    request_id: String,
    operation: String,
    repo_id: String,
    start: Instant,
    #[allow(dead_code)]
    include_timing: bool,
}

impl RequestLog {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn elapsed_millis(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_middleware_new() {
        let logging = LoggingMiddleware::new();
        assert!(logging.include_timing);
        assert!(logging.include_request_id);
    }

    #[test]
    fn test_logging_middleware_default() {
        let logging = LoggingMiddleware::default();
        assert!(logging.include_timing);
    }

    #[test]
    fn test_logging_middleware_builders() {
        let logging = LoggingMiddleware::new()
            .with_level(Level::DEBUG)
            .with_timing(false)
            .with_request_id(false);

        assert_eq!(logging.level, Level::DEBUG);
        assert!(!logging.include_timing);
        assert!(!logging.include_request_id);
    }

    #[test]
    fn test_request_log() {
        let logging = LoggingMiddleware::new();
        let log = logging.log_request_start("req-1", "fetch", "repo-1");

        assert_eq!(log.request_id(), "req-1");
        assert_eq!(log.operation(), "fetch");
        assert_eq!(log.repo_id(), "repo-1");

        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(log.elapsed_millis() >= 1);
    }

    #[test]
    fn test_log_success() {
        let logging = LoggingMiddleware::new();
        let log = logging.log_request_start("req-1", "fetch", "repo-1");
        logging.log_request_success(&log, Some(1000));
    }

    #[test]
    fn test_log_error() {
        let logging = LoggingMiddleware::new();
        let log = logging.log_request_start("req-1", "fetch", "repo-1");
        logging.log_request_error(&log, "test error");
    }

    #[test]
    fn test_log_auth() {
        let logging = LoggingMiddleware::new();
        logging.log_auth_success("req-1", "user-1");
        logging.log_auth_failure("req-1", "invalid token");
    }

    #[test]
    fn test_log_rate_limit() {
        let logging = LoggingMiddleware::new();
        logging.log_rate_limit("req-1", "user-1", "user");
    }
}
