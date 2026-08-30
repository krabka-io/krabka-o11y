use super::{Result, TraceqlError, Value, arithmetic_type_error, i64_to_f64};

pub(crate) fn value_pow(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) if rhs >= 0 => u32::try_from(rhs)
            .ok()
            .and_then(|rhs| lhs.checked_pow(rhs))
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer exponentiation out of range".into())),
        (Value::Int(lhs), Value::Int(rhs)) => {
            Ok(Value::Float(i64_to_f64(lhs)?.powf(i64_to_f64(rhs)?)))
        }
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs.powf(rhs))),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)?.powf(rhs))),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs.powf(i64_to_f64(rhs)?))),
        (lhs, rhs) => arithmetic_type_error("^", &lhs, &rhs),
    }
}
