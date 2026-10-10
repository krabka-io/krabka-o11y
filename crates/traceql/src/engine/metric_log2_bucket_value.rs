use super::{
    Field, Intrinsic, MetricFieldNumber, RecordBatch, Result, Scope, TraceqlError, f64_from_u64,
    metric_field_number,
};

/// Tempo buckets integer attributes at powers of two; duration buckets are
/// based on integer nanoseconds, then expressed in seconds. Floats are not
/// supported by the pinned histogram aggregator.
pub(crate) fn metric_log2_bucket_value(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<f64>> {
    let Some(MetricFieldNumber::Int(value)) = metric_field_number(batch, row, field)? else {
        return Ok(None);
    };
    let Ok(value) = u64::try_from(value) else {
        return Ok(None);
    };
    if value < 2 {
        return Ok(None);
    }
    let bucket = value
        .checked_next_power_of_two()
        .ok_or_else(|| TraceqlError::Exec("histogram bucket exceeds u64".into()))?;
    let bucket = f64_from_u64(bucket)?;
    Ok(Some(
        if matches!(
            field.scope,
            Scope::Intrinsic(
                Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart
            )
        ) {
            bucket / 1_000_000_000.0
        } else {
            bucket
        },
    ))
}
