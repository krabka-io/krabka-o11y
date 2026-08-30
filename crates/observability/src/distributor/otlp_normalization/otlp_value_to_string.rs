use super::*;

pub(crate) fn otlp_value_to_string(value: &OtlpAnyValue) -> String {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => value.clone(),
        OtlpAnyValue::Bool(value) => value.to_string(),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => metadata_value_to_string(value),
        OtlpAnyValue::Array(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP array values serialize to JSON"),
        OtlpAnyValue::Kvlist(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP key-value lists serialize to JSON"),
    }
}
