use super::{Field, MetricFieldNumber, RecordBatch, Result, f64_from_i64, metric_field_number};

/// Extracts the numeric value of a metric fold field for one row.
///
/// This function returns `Ok(None)` when the row's value field is NULL, that
/// is, when the target attribute is absent for that span. The caller can then
/// skip the span instead of folding a false `0.0` into sum, min, max, avg, or
/// histogram. Tempo's Static.Float returns NaN for arrays: none of their
/// elements is admitted as a scalar numeric observation.
pub(crate) fn metric_numeric_value(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<f64>> {
    let value = match metric_field_number(batch, row, field)? {
        Some(MetricFieldNumber::Int(value)) => f64_from_i64(value),
        Some(MetricFieldNumber::Float(value)) => value,
        None => return Ok(None),
    };
    if value.is_nan() {
        return Ok(None);
    }
    Ok(Some(
        if matches!(
            field.scope,
            super::Scope::Intrinsic(super::Intrinsic::Duration)
        ) {
            value / 1_000_000_000.0
        } else {
            value
        },
    ))
}
