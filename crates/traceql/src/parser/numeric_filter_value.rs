use super::{Result, TraceqlError, Value, i64_to_f64};

pub(crate) fn numeric_filter_value(value: Value) -> Result<f64> {
    match value {
        Value::Int(value) => i64_to_f64(value),
        Value::Float(value) => Ok(value),
        other => Err(TraceqlError::Parse(format!(
            "expected numeric filter value, got {other:?}"
        ))),
    }
}
