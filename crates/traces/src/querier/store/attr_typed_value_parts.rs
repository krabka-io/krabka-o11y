use super::AttrValue;

pub(crate) fn attr_typed_value_parts(value: &AttrValue) -> Vec<(String, String)> {
    if let AttrValue::Array(values) = value {
        return values.iter().flat_map(attr_typed_value_parts).collect();
    }
    if matches!(value, AttrValue::Unsupported(_)) {
        return Vec::new();
    }
    vec![match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Float(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
        AttrValue::Unsupported(_) | AttrValue::Array(_) => unreachable!("arrays handled above"),
    }]
}
