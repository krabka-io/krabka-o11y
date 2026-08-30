use super::{
    Array, AsArray, DataType, Field, RecordBatch, Result, TraceqlError, f64_from_i64,
    metric_field_column,
};

/// Extracts the numeric value of a metric fold field for one row.
///
/// This function returns `Ok(None)` when the row's value field is NULL, that
/// is, when the target attribute is absent for that span. The caller can then
/// skip the span instead of folding a false `0.0` into sum, min, max, avg, or
/// histogram.
pub(crate) fn metric_numeric_value(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<f64>> {
    let column = metric_field_column(field)?;
    let array = batch
        .column_by_name(&column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int64 => f64_from_i64(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::Int32 => f64::from(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row),
        ),
        DataType::Float64 => array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row),
        other => {
            return Err(TraceqlError::Unsupported(format!(
                "metrics fold field {field:?} has non-numeric type {other:?}"
            )));
        }
    };
    Ok(Some(value))
}
