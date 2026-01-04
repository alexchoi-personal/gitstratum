use metrics::{counter, histogram};

pub fn record_auth_attempt(method: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    counter!("gitstratum_auth_attempts_total", "method" => method.to_string(), "status" => status)
        .increment(1);
}

pub fn record_auth_duration(method: &str, duration_secs: f64) {
    histogram!("gitstratum_auth_duration_seconds", "method" => method.to_string())
        .record(duration_secs);
}

pub fn record_permission_check(allowed: bool) {
    let result = if allowed { "allowed" } else { "denied" };
    counter!("gitstratum_permission_checks_total", "result" => result).increment(1);
}

pub fn record_rate_limit_exceeded(key_type: &str) {
    counter!("gitstratum_rate_limit_exceeded_total", "type" => key_type.to_string()).increment(1);
}
