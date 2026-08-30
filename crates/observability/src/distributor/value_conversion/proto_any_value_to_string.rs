use super::*;

pub(crate) fn proto_any_value_to_string(value: &proto_any_value::Value) -> String {
    match value {
        proto_any_value::Value::StringValue(value) => value.clone(),
        proto_any_value::Value::BoolValue(value) => value.to_string(),
        proto_any_value::Value::IntValue(value) => value.to_string(),
        proto_any_value::Value::DoubleValue(value) => value.to_string(),
        proto_any_value::Value::BytesValue(value) => hex_string(value),
        proto_any_value::Value::ArrayValue(value) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(proto_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP protobuf array values serialize to JSON"),
        proto_any_value::Value::KvlistValue(value) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.clone(),
                        attribute
                            .value
                            .as_ref()
                            .map_or(Value::Null, proto_value_to_json),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP protobuf key-value lists serialize to JSON"),
        proto_any_value::Value::StringValueStrindex(value) => value.to_string(),
    }
}
