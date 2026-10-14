use super::{ManifestTimeRange, Time, TimeExt};

pub(crate) fn normalize_refresh_range(
    start_ms: i64,
    end_ms: i64,
    lookback: Time,
    now_ms: i64,
) -> ManifestTimeRange {
    if start_ms == i64::MIN && end_ms == i64::MAX {
        return ManifestTimeRange {
            start_ms: now_ms.saturating_sub(lookback.millis_i64()),
            end_ms: i64::MAX,
        };
    }
    ManifestTimeRange { start_ms, end_ms }
}
