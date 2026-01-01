use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        assert!(ts > 0);

        let ts2 = current_timestamp();
        assert!(ts2 >= ts);
    }

    #[test]
    fn test_current_timestamp_millis() {
        let ts = current_timestamp_millis();
        assert!(ts > 0);

        let ts2 = current_timestamp_millis();
        assert!(ts2 >= ts);
    }

    #[test]
    fn test_timestamp_is_reasonable() {
        let ts = current_timestamp();
        assert!(ts > 1700000000);
        assert!(ts < 2000000000);
    }

    #[test]
    fn test_timestamp_millis_is_reasonable() {
        let ts = current_timestamp_millis();
        assert!(ts > 1700000000000);
        assert!(ts < 2000000000000);
    }

    #[test]
    fn test_millis_greater_than_secs() {
        let ts_secs = current_timestamp();
        let ts_millis = current_timestamp_millis();
        assert!(ts_millis >= ts_secs * 1000);
        assert!(ts_millis < (ts_secs + 1) * 1000);
    }
}
