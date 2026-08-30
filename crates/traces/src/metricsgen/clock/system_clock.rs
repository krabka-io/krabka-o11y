use super::{Clock, SystemTime, UNIX_EPOCH};

/// Production clock.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
    }
}
