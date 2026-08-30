use super::*;

pub(crate) fn unsupported_metric_pipeline() -> TraceqlError {
    TraceqlError::Unsupported("traceql metrics: expected supported *_over_time() metric".into())
}
