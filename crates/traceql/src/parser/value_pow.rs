use super::{ArithmeticOperands, Result, TraceqlError, Value, arithmetic_type_error, i64_to_f64};

pub(crate) fn value_pow(lhs: Value, rhs: Value) -> Result<Value> {
    let operands = ArithmeticOperands { lhs, rhs };
    match operands {
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } if rhs >= 0 => u32::try_from(rhs)
            .ok()
            .and_then(|rhs| lhs.checked_pow(rhs))
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer exponentiation out of range".into())),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Int(rhs),
        } => Ok(Value::Float(i64_to_f64(lhs)?.powf(i64_to_f64(rhs)?))),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(lhs.powf(rhs))),
        ArithmeticOperands {
            lhs: Value::Int(lhs),
            rhs: Value::Float(rhs),
        } => Ok(Value::Float(i64_to_f64(lhs)?.powf(rhs))),
        ArithmeticOperands {
            lhs: Value::Float(lhs),
            rhs: Value::Int(rhs),
        } => Ok(Value::Float(lhs.powf(i64_to_f64(rhs)?))),
        ArithmeticOperands { lhs, rhs } => arithmetic_type_error("^", &lhs, &rhs),
    }
}
