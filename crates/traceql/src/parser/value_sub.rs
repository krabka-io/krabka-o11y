use super::*;

pub(crate) fn value_sub(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) => lhs
            .checked_sub(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer subtraction out of range".into())),
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs - rhs)),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)? - rhs)),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs - i64_to_f64(rhs)?)),
        (Value::Duration(lhs), Value::Duration(rhs)) => lhs
            .checked_sub(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration subtraction out of range".into())),
        (lhs, rhs) => arithmetic_type_error("-", &lhs, &rhs),
    }
}
