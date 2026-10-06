use super::{
    COL_TRACE_ID, Field, RecordBatch, Result, TraceMetricExemplar, bytes_to_hex, fixed_16,
};

pub(crate) fn metric_exemplar(
    batch: &RecordBatch,
    row: usize,
    timestamp_ns: i64,
    value: f64,
    fields: &[Field],
) -> Result<TraceMetricExemplar> {
    // Tempo uses its unpadded hexadecimal trace intrinsic, plus attributes
    // fetched for the query. Span IDs are included only when queried.
    let trace_id = bytes_to_hex(&fixed_16(batch, COL_TRACE_ID, row)?);
    let trace_id = trace_id.trim_start_matches('0');
    let mut labels = vec![(
        "trace:id".into(),
        if trace_id.is_empty() { "0" } else { trace_id }.into(),
    )];
    let (projected, label_types) = super::metric_labels(batch, row, fields)?;
    labels.extend(projected.into_iter().filter(|(key, _)| key != "trace:id"));
    Ok(TraceMetricExemplar {
        labels,
        label_types,
        value,
        timestamp_ns,
    })
}
