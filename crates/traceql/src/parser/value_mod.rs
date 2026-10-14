use super::{ArithmeticOperands, Result, TraceqlError, Value, arithmetic_type_error};

pub(crate) fn value_mod(lhs: Value, rhs: Value) -> Result<Value> {
    let operands = ArithmeticOperands { lhs, rhs };
    match operands {
        ArithmeticOperands {
            rhs: Value::Int(0) | Value::Duration(0),
            ..
        } => Err(TraceqlError::Parse("modulo by zero".into())),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } => lhs
            .checked_rem(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer modulo out of range".into())),
        ArithmeticOperands {
            lhs: Value::Duration(lhs),
            rhs: Value::Duration(rhs),
        } => lhs
            .checked_rem(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration modulo out of range".into())),
        ArithmeticOperands { lhs, rhs } => arithmetic_type_error("%", &lhs, &rhs),
    }
}
