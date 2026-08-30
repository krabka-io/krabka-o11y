use crate::{BTreeMap, DistributorError, OtlpAnyValue, ProtoAnyValue, Value, proto_any_value};

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

pub(crate) fn proto_value_to_string(value: &ProtoAnyValue) -> String {
    value
        .value
        .as_ref()
        .map(proto_any_value_to_string)
        .unwrap_or_default()
}

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

pub(crate) fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

pub(crate) fn parse_structured_metadata(
    metadata: Option<&Value>,
) -> Result<BTreeMap<String, String>, DistributorError> {
    let Some(metadata) = metadata else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(metadata) = metadata else {
        return Err(DistributorError::InvalidStructuredMetadata);
    };

    metadata
        .iter()
        .map(|(name, value)| {
            let value = value
                .as_str()
                .ok_or(DistributorError::InvalidStructuredMetadata)?;
            Ok((name.clone(), value.to_string()))
        })
        .collect::<Result<BTreeMap<_, _>, DistributorError>>()
}

pub(crate) fn metadata_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}
