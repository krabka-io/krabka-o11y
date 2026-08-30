/// W3C trace-context header key that the distributor's ingest span puts on WAL
/// records. The consume side uses it to continue the same distributed trace.
pub(crate) const TRACEPARENT_HEADER: &str = "traceparent";
