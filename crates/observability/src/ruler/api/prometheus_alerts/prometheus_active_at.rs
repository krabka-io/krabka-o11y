use super::{OffsetDateTime, Rfc3339};

pub(crate) fn prometheus_active_at(timestamp_ns: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ns))
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}
