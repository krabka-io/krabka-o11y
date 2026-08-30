use super::{AnyValue, any_to_attr, AttrValue};

pub(crate) fn any_to_text(value: &AnyValue) -> Option<String> {
    match any_to_attr(value)? {
        AttrValue::Str(value) => Some(value),
        AttrValue::Int(value) => Some(value.to_string()),
        AttrValue::Double(value) => Some(value.to_string()),
        AttrValue::Bool(value) => Some(value.to_string()),
        AttrValue::Bytes(value) => Some(hex::encode(value)),
    }
}
