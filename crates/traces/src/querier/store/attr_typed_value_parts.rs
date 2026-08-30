use super::AttrValue;

pub(crate) fn attr_typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Float(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
    }
}
