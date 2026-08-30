use super::{RecordBatch, Result, QueryResult, scalar_math, Array, Float64Array, PromqlError, Int64Array, labels_from_rate_batch, InstantSample, SampleValue};

/// Assembles per-row scalar-math projection output batches into a result.
///
/// Output rows carry the metadata-free label columns plus one `value` column.
/// The leaf already dropped the metric name, this function reads the label set
/// directly from the batch, and the eval timestamp is reattached. The function
/// keeps every row: the scalar-math functions never drop a float sample, so
/// `f(NaN)` and `sqrt(-1)` surface as `NaN`. This matches the interpreter's
/// `eval_unary_float_call`, `eval_clamp_call`, and `eval_round_call`.
pub(crate) fn assemble_scalar_math_batches(
    batches: &[RecordBatch],
    _time_ms: i64,
) -> Result<QueryResult> {
    let mut samples = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(scalar_math::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("scalar-math projection missing Float64 value column".to_string())
            })?;
        let sample_timestamps = batch
            .column_by_name(scalar_math::SAMPLE_TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec(
                    "scalar-math projection missing Int64 sample-timestamp column".to_string(),
                )
            })?;
        for row in 0..batch.num_rows() {
            // The scalar-math projection carries label (`Utf8`) columns plus the
            // float `value` result and the Int64 `sample_timestamp`;
            // `labels_from_rate_batch` reads only the string label columns (it
            // skips the Int64 timestamp), reconstructing the labelset.
            let labels = labels_from_rate_batch(batch, row);
            samples.push(InstantSample {
                labels,
                // Scalar-math functions report the inner sample's timestamp
                // unchanged (the interpreter keeps `sample.ts_ms`).
                ts_ms: sample_timestamps.value(row),
                value: SampleValue::Float(values.value(row)),
            });
        }
    }
    Ok(QueryResult::InstantVector(samples))
}
