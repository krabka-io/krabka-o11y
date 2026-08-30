use super::*;

pub(crate) fn arithmetic_type_error(op: &str, lhs: &Value, rhs: &Value) -> Result<Value> {
    Err(TraceqlError::Parse(format!(
        "operator {op} is not supported for {lhs:?} and {rhs:?}"
    )))
}
