use super::{Result, TraceqlError, Value};

pub(crate) fn value_neg(value: Value) -> Result<Value> {
    match value {
        Value::Int(value) => value
            .checked_neg()
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer negation out of range".into())),
        Value::Float(value) => Ok(Value::Float(-value)),
        Value::Duration(value) => value
            .checked_neg()
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration negation out of range".into())),
        other => Err(TraceqlError::Parse(format!(
            "unary - is not supported for {other:?}"
        ))),
    }
}
