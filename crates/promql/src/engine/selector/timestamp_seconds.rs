use super::*;

pub(crate) fn timestamp_seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms.to_f64().unwrap_or(f64::MAX) / 1000.0
}
