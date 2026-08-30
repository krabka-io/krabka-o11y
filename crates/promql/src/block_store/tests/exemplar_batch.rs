use super::*;

pub(crate) fn exemplar_batch(
    fingerprint: u64,
    timestamp_ms: i64,
    value: f64,
    trace_id: &str,
    span_id: &str,
    label_name: &str,
    label_value: &str,
) -> RecordBatch {
    exemplar_batch_from_rows(&[(
        fingerprint,
        timestamp_ms,
        value,
        trace_id,
        span_id,
        label_name,
        label_value,
    )])
}
