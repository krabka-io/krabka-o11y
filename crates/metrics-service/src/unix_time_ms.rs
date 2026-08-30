use super::{SystemTime, UNIX_EPOCH, duration_ms};

pub(crate) fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, duration_ms)
}
