use super::AttrValue;

/// Flattens `value` into Tempo's `(type, value)` tag-value pairs, recursing
/// into arrays and dropping the opaque values Tempo does not list.
#[must_use]
pub fn typed_value_parts(value: &AttrValue) -> Vec<(String, String)> {
    if let AttrValue::Array(values) = value {
        return values.iter().flat_map(typed_value_parts).collect();
    }
    if matches!(value, AttrValue::Unsupported(_)) {
        return Vec::new();
    }
    vec![match value {
        AttrValue::Str(v) => ("string".to_string(), v.clone()),
        AttrValue::Int(v) => ("int".to_string(), v.to_string()),
        AttrValue::Float(v) => ("float".to_string(), v.to_string()),

        AttrValue::Bool(v) => ("bool".to_string(), v.to_string()),
        AttrValue::Unsupported(_) | AttrValue::Array(_) => unreachable!("arrays handled above"),
    }]
}
