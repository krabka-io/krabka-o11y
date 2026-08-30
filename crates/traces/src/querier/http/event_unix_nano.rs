use super::*;

/// An event's absolute instant: the span's start coordinate advanced by the
/// event's offset into the span.
pub(crate) fn event_unix_nano(span: &SpanRef, event: &krabka_traceql::EventRef) -> u64 {
    span.start_time_unix_nano
        .saturating_add(u64::try_from(event.time_since_start.nanos_i64()).unwrap_or(0))
}
