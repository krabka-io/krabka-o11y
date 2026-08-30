use super::*;

pub(crate) fn value_div(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (_, Value::Int(0) | Value::Float(0.0)) => {
            Err(TraceqlError::Parse("division by zero".into()))
        }
        (Value::Int(lhs), Value::Int(rhs)) => {
            let rem = lhs
                .checked_rem(rhs)
                .ok_or_else(|| TraceqlError::Parse("integer division out of range".into()))?;
            if rem == 0 {
                let quot = lhs
                    .checked_div(rhs)
                    .ok_or_else(|| TraceqlError::Parse("integer division out of range".into()))?;
                Ok(Value::Int(quot))
            } else {
                Ok(Value::Float(i64_to_f64(lhs)? / i64_to_f64(rhs)?))
            }
        }
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs / rhs)),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)? / rhs)),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs / i64_to_f64(rhs)?)),
        (Value::Duration(lhs), Value::Int(rhs)) => lhs
            .checked_div(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration division out of range".into())),
        (lhs, rhs) => arithmetic_type_error("/", &lhs, &rhs),
    }
}
