use super::*;

pub(crate) fn value_sql(value: &Value) -> Result<String> {
    match value {
        Value::Str(v) => Ok(string_lit(v)),
        Value::Int(v) | Value::Duration(v) => Ok(v.to_string()),
        Value::Float(v) => {
            if !v.is_finite() {
                return Err(TraceqlError::Plan("comparison value is not finite".into()));
            }
            Ok(v.to_string())
        }
        Value::Bool(v) => Ok(v.to_string()),
        Value::Nil => Err(TraceqlError::Plan(
            "nil only supports equality comparisons".into(),
        )),
    }
}
