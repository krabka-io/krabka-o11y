use super::{Time, TimeExt};

pub(crate) fn normalize_refresh_range(
    start_ms: i64,
    end_ms: i64,
    lookback: Time,
    now_ms: i64,
) -> (i64, i64) {
    if start_ms == i64::MIN && end_ms == i64::MAX {
        return (now_ms.saturating_sub(lookback.millis_i64()), i64::MAX);
    }
    (start_ms, end_ms)
}
