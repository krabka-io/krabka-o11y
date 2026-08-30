use super::{
    COL_SPAN_ID, COL_TRACE_ID, RecordBatch, Result, TraceMetricExemplar, bytes_to_hex, fixed_8,
    fixed_16,
};

pub(crate) fn metric_exemplar(
    batch: &RecordBatch,
    row: usize,
    timestamp_ns: i64,
    value: f64,
) -> Result<TraceMetricExemplar> {
    Ok(TraceMetricExemplar {
        labels: vec![
            (
                "trace_id".into(),
                bytes_to_hex(&fixed_16(batch, COL_TRACE_ID, row)?),
            ),
            (
                "span_id".into(),
                bytes_to_hex(&fixed_8(batch, COL_SPAN_ID, row)?),
            ),
        ],
        value,
        timestamp_ns,
    })
}
