use super::*;

/// A span's end instant: its start coordinate advanced by its duration.
pub(crate) fn span_end_unix_nano(span: &SpanRef) -> u64 {
    span.start_time_unix_nano
        .saturating_add(u64::try_from(span.duration.nanos_i64()).unwrap_or(0))
}
