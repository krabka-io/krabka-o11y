use super::{
    Array, AsArray, DataType, Field, Intrinsic, RecordBatch, Result, Scope, TraceqlError,
    f64_from_u64, metric_field_column,
};

/// Tempo buckets integer attributes at powers of two; duration buckets are
/// based on integer nanoseconds, then expressed in seconds. Floats are not
/// supported by the pinned histogram aggregator.
pub(crate) fn metric_log2_bucket_value(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<f64>> {
    let values = super::compare_row(batch, row, super::UnixNano(0))?;
    let raw = super::compare_row_attr_values(&values, &field.scope, &field.key);
    let value = if let Some(value) = raw.first() {
        match value {
            super::AttrValue::Int(value) => *value,
            _ => return Ok(None),
        }
    } else {
        let column = metric_field_column(field)?;
        let array = batch
            .column_by_name(&column)
            .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
        if array.is_null(row) {
            return Ok(None);
        }
        match array.data_type() {
            DataType::Int64 => array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
            DataType::Int32 => i64::from(
                array
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .value(row),
            ),
            _ => return Ok(None),
        }
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
        if matches!(field.scope, Scope::Intrinsic(Intrinsic::Duration)) {
            bucket / 1_000_000_000.0
        } else {
            bucket
        },
    ))
}
