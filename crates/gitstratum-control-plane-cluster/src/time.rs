use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[inline]
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_current_timestamp_secs_increases() {
        let t1 = current_timestamp_secs();
        thread::sleep(Duration::from_millis(10));
        let t2 = current_timestamp_secs();
        assert!(t2 >= t1);
    }

    #[test]
    fn test_current_timestamp_millis_increases() {
        let t1 = current_timestamp_millis();
        thread::sleep(Duration::from_millis(10));
        let t2 = current_timestamp_millis();
        assert!(t2 > t1);
    }

    #[test]
    fn test_timestamps_are_reasonable() {
        let secs = current_timestamp_secs();
        let millis = current_timestamp_millis();

        assert!(secs > 1_700_000_000);
        assert!(millis > 1_700_000_000_000);
        assert_eq!(secs, millis / 1000);
    }
}
