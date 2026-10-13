use super::{AttrValue, TypedValue};

/// Flattens `value` into Tempo's `(type, value)` tag-value pairs, recursing
/// into arrays and dropping the opaque values Tempo does not list.
#[must_use]
pub fn typed_value_parts(value: &AttrValue) -> Vec<TypedValue> {
    if let AttrValue::Array(values) = value {
        return values.iter().flat_map(typed_value_parts).collect();
    }
    if matches!(value, AttrValue::Unsupported(_)) {
        return Vec::new();
    }
    vec![match value {
        AttrValue::Str(v) => TypedValue {
            type_: "string".to_string(),
            value: v.clone(),
        },
        AttrValue::Int(v) => TypedValue {
            type_: "int".to_string(),
            value: v.to_string(),
        },
        AttrValue::Float(v) => TypedValue {
            type_: "float".to_string(),
            value: v.to_string(),
        },

        AttrValue::Bool(v) => TypedValue {
            type_: "bool".to_string(),
            value: v.to_string(),
        },
        AttrValue::Unsupported(_) | AttrValue::Array(_) => unreachable!("arrays handled above"),
    }]
}
