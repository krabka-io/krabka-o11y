use super::{ProtoAnyValue, Value, hex_string, proto_any_value};

pub(crate) fn proto_value_to_json(value: &ProtoAnyValue) -> Value {
    match value.value.as_ref() {
        Some(proto_any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(proto_any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(proto_any_value::Value::IntValue(value)) => Value::Number((*value).into()),
        Some(proto_any_value::Value::DoubleValue(value)) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        Some(proto_any_value::Value::BytesValue(value)) => Value::String(hex_string(value)),
        Some(proto_any_value::Value::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(proto_value_to_json).collect())
        }
        Some(proto_any_value::Value::KvlistValue(value)) => Value::Object(
            value
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
                .collect(),
        ),
        Some(proto_any_value::Value::StringValueStrindex(value)) => Value::Number((*value).into()),
        None => Value::Null,
    }
}
