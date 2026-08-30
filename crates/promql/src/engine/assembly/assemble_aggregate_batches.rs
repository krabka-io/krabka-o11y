use super::{AGGREGATE_VALUE_COLUMN, Array, Float64Array, InstantSample, PromqlError, QueryResult, RecordBatch, Result, SampleValue, labels_from_rate_batch};

/// Assembles simple-aggregation output batches into a result.
///
/// Output rows carry exactly the grouping label columns plus `value`. This
/// function reads the label set directly from the batch, with no fingerprint
/// lookup, and reattaches the eval timestamp. An empty grouping, which is
/// `by ()` or no modifier, returns one row with an empty label set.
///
/// A NULL aggregate result means the group had no value-bearing input. This
/// happens when every member was a no-value series that the pre-aggregate NULL
/// filter dropped, or when the NaN-ignoring `min`/`max` UDAF saw only nulls. The
/// function drops such a result, which matches the interpreter: the interpreter
/// forms no group when no sample reaches it. A non-null NaN result is a genuine
/// aggregated NaN, for example a `sum` over a group that holds a genuine NaN or
/// an all-NaN `min`/`max` group, and the function KEEPS it.
pub(crate) fn assemble_aggregate_batches(
    batches: &[RecordBatch],
    time_ms: i64,
) -> Result<QueryResult> {
    let mut samples = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(AGGREGATE_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("aggregate projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL aggregate = no value-bearing input for the group: drop it
            // (the interpreter never forms such a group). A non-null NaN is a
            // genuine aggregated NaN: keep it.
            if values.is_null(row) {
                continue;
            }
            // The grouping labels are exactly the batch's non-`value` Utf8
            // columns; `labels_from_rate_batch` reads precisely those.
            let labels = labels_from_rate_batch(batch, row);
            samples.push(InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(values.value(row)),
            });
        }
    }
    Ok(QueryResult::InstantVector(samples))
}
