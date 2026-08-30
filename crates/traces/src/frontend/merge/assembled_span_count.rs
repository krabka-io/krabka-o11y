use super::*;

/// Total span count of a typed by-id body. This is a helper for callers and
/// tests.
#[must_use]
pub fn assembled_span_count(trace: &TraceByIdResponseJson) -> usize {
    trace.span_count()
}
