use super::AttrValue;

pub(crate) fn attr_value_display(value: &AttrValue) -> String {
    match value {
        AttrValue::Unsupported(_) => String::new(),
        AttrValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(attr_value_display)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Float(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
    }
}
