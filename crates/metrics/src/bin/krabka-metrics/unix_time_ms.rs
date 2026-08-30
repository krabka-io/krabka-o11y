use super::{SystemTime, UNIX_EPOCH};

// cargo-mutants: wall-clock read; no deterministic assertion.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis().min(i64::MAX as u128)).unwrap_or(i64::MAX)
        })
}
