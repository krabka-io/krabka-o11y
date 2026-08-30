use super::{RecordBatch, BTreeMap, SeriesFingerprint, Labels, Result, QueryResult, rate_range, Array, Float64Array, PromqlError, labels_from_rate_batch, InstantSample, labels_without_metric_name, SampleValue};

/// Assembles rate-family projection output batches into a result.
///
/// Output rows carry label columns plus one `value` column. This function
/// reattaches the eval timestamp and drops the metric name. It also drops NULL
/// rows, which are the UDF's "no value" marker for a window with too few
/// samples. This is exactly how the interpreter omits no-value series. A
/// non-null NaN row is a genuine NaN value, so the function KEEPS it and
/// propagates it.
pub(crate) fn assemble_rate_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
) -> Result<QueryResult> {
    let mut by_fp: BTreeMap<SeriesFingerprint, f64> = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(rate_range::RATE_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("rate projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL is the no-value marker (the series has no value at this
            // step): drop it. A non-null NaN is a genuine NaN value: keep it.
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            let fp = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fp, value);
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, value)| {
            labels_by_fp.get(&fp).map(|labels| InstantSample {
                // Rate-family results drop the metric name, matching
                // `eval_range_function_call`'s `labels_without_metric_name`.
                labels: labels_without_metric_name(labels),
                ts_ms: time_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}
