use super::{ArithmeticOperands, Result, TraceqlError, Value, arithmetic_type_error, i64_to_f64};

pub(crate) fn value_mul(lhs: Value, rhs: Value) -> Result<Value> {
    let operands = ArithmeticOperands { lhs, rhs };
    match operands {
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } => lhs
            .checked_mul(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer multiplication out of range".into())),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(lhs * rhs)),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(i64_to_f64(lhs)? * rhs)),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Int(rhs),
        } => Ok(Value::Float(lhs * i64_to_f64(rhs)?)),
        ArithmeticOperands {
            lhs: Value::Duration(lhs),
            rhs: Value::Int(rhs),
        }
        | ArithmeticOperands {
            lhs: Value::Int(rhs),
            rhs: Value::Duration(lhs),
        } => lhs
            .checked_mul(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration multiplication out of range".into())),
        ArithmeticOperands { lhs, rhs } => arithmetic_type_error("*", &lhs, &rhs),
    }
}
