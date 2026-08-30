use super::*;

pub(crate) fn attr_value_json(value: &AttrValue) -> Value {
    match value {
        AttrValue::Str(v) => json!({"stringValue": v}),
        AttrValue::Int(v) => json!({"intValue": v.to_string()}),
        AttrValue::Float(v) => json!({"doubleValue": v}),
        AttrValue::Bool(v) => json!({"boolValue": v}),
    }
}
