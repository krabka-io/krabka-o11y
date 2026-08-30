use super::*;

/// Default wall clock backed by [`std::time::SystemTime`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_millis(&self) -> i64 {
        let now = std::time::SystemTime::now();
        match now.duration_since(std::time::UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
            // Clock is before the Unix epoch; treat as time zero.
            Err(_) => 0,
        }
    }
}
