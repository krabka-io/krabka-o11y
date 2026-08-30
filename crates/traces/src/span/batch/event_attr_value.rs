use super::AttrValue;

pub(crate) fn event_attr_value(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Double(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
        AttrValue::Bytes(value) => hex::encode(value),
    }
}
