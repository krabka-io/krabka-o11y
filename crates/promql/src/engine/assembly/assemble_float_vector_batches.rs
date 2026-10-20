use super::{
    Array, BTreeMap, Float64Array, InstantSample, Labels, PromqlError, QueryResult, RecordBatch,
    Result, SampleValue, SeriesFingerprint, labels_from_rate_batch,
};

pub(super) fn assemble_float_vector_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
    value_column: &str,
    missing_column_error: &str,
    drop_name: bool,
) -> Result<QueryResult> {
    let mut by_fp = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(value_column)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| PromqlError::Exec(missing_column_error.to_string()))?;
        for row in 0..batch.num_rows() {
            if values.is_null(row) {
                continue;
            }
            let fingerprint = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fingerprint, values.value(row));
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fingerprint, value)| {
            labels_by_fp.get(&fingerprint).map(|labels| InstantSample {
                labels: labels.clone(),
                ts_ms: time_ms,
                value: SampleValue::Float(value),
                drop_name,
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}
