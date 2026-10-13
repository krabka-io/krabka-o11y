use super::{ArithmeticOperands, Result, TraceqlError, Value, arithmetic_type_error, i64_to_f64};

pub(crate) fn value_add(lhs: Value, rhs: Value) -> Result<Value> {
    let operands = ArithmeticOperands { lhs, rhs };
    match operands {
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } => lhs
            .checked_add(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer addition out of range".into())),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(lhs + rhs)),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(i64_to_f64(lhs)? + rhs)),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Int(rhs),
        } => Ok(Value::Float(lhs + i64_to_f64(rhs)?)),
        ArithmeticOperands {
            lhs: Value::Duration(lhs),
            rhs: Value::Duration(rhs),
        } => lhs
            .checked_add(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration addition out of range".into())),
        ArithmeticOperands { lhs, rhs } => arithmetic_type_error("+", &lhs, &rhs),
    }
}
