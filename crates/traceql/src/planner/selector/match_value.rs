use super::{MatchValue, Value};

pub(crate) fn match_value(value: &Value) -> MatchValue {
    match value {
        Value::Str(v) => MatchValue::Str(v.clone()),
        Value::Int(v) | Value::Duration(v) => MatchValue::Int(*v),
        Value::Float(v) => MatchValue::Float(*v),
        Value::Bool(v) => MatchValue::Bool(*v),
        Value::Nil => MatchValue::Nil,
    }
}
