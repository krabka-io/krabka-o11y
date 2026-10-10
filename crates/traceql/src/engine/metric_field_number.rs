use super::{
    Array, AsArray, AttrValue, DataType, Field, RecordBatch, Result, TraceqlError,
    metric_field_column,
};

/// The numeric value of a metric fold field for one row, in its stored type.
pub(crate) enum MetricFieldNumber {
    Int(i64),
    Float(f64),
}

/// Looks up the numeric value of a metric fold field for one row.
///
/// This function returns `Ok(None)` when the field has more than one attribute
/// value (Tempo admits no array element as a scalar observation), when the
/// value is not numeric, or when the row's projected column is NULL, that is,
/// when the attribute is absent.
pub(crate) fn metric_field_number(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<MetricFieldNumber>> {
    let values = super::compare_row(batch, row, super::UnixNano(0))?;
    let raw = super::compare_row_attr_values(&values, &field.scope, &field.key);
    if raw.len() > 1 {
        return Ok(None);
    }
    if let Some(value) = raw.first() {
        return Ok(match value {
            AttrValue::Int(value) => Some(MetricFieldNumber::Int(*value)),
            AttrValue::Float(value) => Some(MetricFieldNumber::Float(*value)),
            AttrValue::Unsupported(_)
            | AttrValue::Array(_)
            | AttrValue::Bool(_)
            | AttrValue::Str(_) => None,
        });
    }
    let column = metric_field_column(field)?;
    let array = batch
        .column_by_name(&column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(match array.data_type() {
        DataType::Int64 => Some(MetricFieldNumber::Int(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        )),
        DataType::Int32 => Some(MetricFieldNumber::Int(i64::from(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row),
        ))),
        DataType::Float64 => Some(MetricFieldNumber::Float(
            array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        )),
        _ => None,
    })
}
