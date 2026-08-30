use super::*;

pub(crate) fn otlp_value_to_json(value: &OtlpAnyValue) -> Value {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => Value::String(value.clone()),
        OtlpAnyValue::Bool(value) => Value::Bool(*value),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => value.clone(),
        OtlpAnyValue::Array(value) => Value::Array(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect(),
        ),
        OtlpAnyValue::Kvlist(value) => Value::Object(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect(),
        ),
    }
}
