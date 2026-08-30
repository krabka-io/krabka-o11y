use super::*;

/// Assembles `*_over_time` projection output batches into a result.
///
/// Output rows carry label columns plus one `value` column. This function
/// reattaches the eval timestamp and drops NULL rows, which are the UDF's "no
/// value" marker for an empty window. This is exactly how the interpreter omits
/// no-value series. A non-null NaN row is a genuine NaN value, so the function
/// KEEPS it and propagates it.
///
/// `preserve_metric_name` keeps `__name__` only for `last_over_time`. Every
/// other family drops it, which matches the interpreter's `eval_over_time_call`
/// and `OverTimeFn::preserves_metric_name`.
pub(crate) fn assemble_over_time_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
    preserve_metric_name: bool,
) -> Result<QueryResult> {
    let mut by_fp: BTreeMap<SeriesFingerprint, f64> = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(over_time_range::OVER_TIME_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("over_time projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL is the no-value marker (an empty window): drop it. A non-null
            // NaN is a genuine NaN value: keep it.
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            // The over_time projection carries only label (`Utf8`) columns plus
            // the float `value` result, so `labels_from_rate_batch` (which reads
            // exactly that shape) reconstructs the fingerprint.
            let fp = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fp, value);
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, value)| {
            labels_by_fp.get(&fp).map(|labels| {
                let labels = if preserve_metric_name {
                    labels.clone()
                } else {
                    labels_without_metric_name(labels)
                };
                InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(value),
                }
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}
