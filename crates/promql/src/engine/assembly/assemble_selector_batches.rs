use super::{
    Array, BTreeMap, Float64Array, InstantSample, Int64Array, Labels, PromqlError, QueryResult,
    RecordBatch, Result, SampleValue, SeriesFingerprint, labels_from_batch, leaf,
};

/// Assembles instant-vector-selector output batches into a result.
///
/// Output rows carry label columns plus `timestamp`, `value`, and
/// `sample_timestamp`. `sample_timestamp` holds the selected sample's true
/// timestamp. This function recovers the result labels from `labels_by_fp`,
/// keyed by the row's reconstructed fingerprint.
pub(crate) fn assemble_selector_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
) -> Result<QueryResult> {
    // InstantManipulate emits at most one row per series (the single grid step).
    let mut by_fp: BTreeMap<SeriesFingerprint, (i64, f64)> = BTreeMap::new();
    for batch in batches {
        let sample_timestamps = batch
            .column_by_name(leaf::SAMPLE_TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Int64 sample-timestamp column".to_string())
            })?;
        let values = batch
            .column_by_name(leaf::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            let fp = labels_from_batch(batch, row).fingerprint();
            let ts_ms = sample_timestamps.value(row);
            let value = values.value(row);
            by_fp
                .entry(fp)
                .and_modify(|latest| {
                    if ts_ms > latest.0 {
                        *latest = (ts_ms, value);
                    }
                })
                .or_insert((ts_ms, value));
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, (ts_ms, value))| {
            labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                labels,
                ts_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}
