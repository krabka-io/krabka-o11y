use super::AttrValue;

pub(crate) fn typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Double(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
        AttrValue::Bytes(value) => ("string".into(), hex::encode(value)),
    }
}
