use super::*;

pub(crate) fn attr_value_display(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Float(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
    }
}
