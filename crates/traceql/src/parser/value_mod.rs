use super::{Result, TraceqlError, Value, arithmetic_type_error};

pub(crate) fn value_mod(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (_, Value::Int(0) | Value::Duration(0)) => {
            Err(TraceqlError::Parse("modulo by zero".into()))
        }
        (Value::Int(lhs), Value::Int(rhs)) => lhs
            .checked_rem(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer modulo out of range".into())),
        (Value::Duration(lhs), Value::Duration(rhs)) => lhs
            .checked_rem(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration modulo out of range".into())),
        (lhs, rhs) => arithmetic_type_error("%", &lhs, &rhs),
    }
}
