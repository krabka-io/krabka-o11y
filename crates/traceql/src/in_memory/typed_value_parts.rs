use super::*;

pub(crate) fn typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(v) => ("string".to_string(), v.clone()),
        AttrValue::Int(v) => ("int".to_string(), v.to_string()),
        AttrValue::Float(v) => ("float".to_string(), v.to_string()),
        AttrValue::Bool(v) => ("bool".to_string(), v.to_string()),
    }
}
