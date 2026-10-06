use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value,
};
use serde::{Deserialize as _, de::Error as _};

pub(super) fn encode(value: &AnyValue) -> serde_json::Value {
    let mut encoded = serde_json::to_value(value).expect("OTLP AnyValue serializes");
    match &value.value {
        Some(Value::DoubleValue(number)) if !number.is_finite() => {
            encoded["doubleValue"] = serde_json::Value::String(
                if number.is_nan() {
                    "NaN"
                } else if number.is_sign_positive() {
                    "Infinity"
                } else {
                    "-Infinity"
                }
                .into(),
            );
        }
        Some(Value::ArrayValue(array)) => {
            encoded["arrayValue"]["values"] =
                serde_json::Value::Array(array.values.iter().map(encode).collect());
        }
        Some(Value::KvlistValue(list)) => {
            encoded["kvlistValue"]["values"] = serde_json::Value::Array(
                list.values
                    .iter()
                    .map(|kv| {
                        let mut encoded =
                            serde_json::to_value(kv).expect("OTLP KeyValue serializes");
                        if let Some(value) = &kv.value {
                            encoded["value"] = encode(value);
                        }
                        encoded
                    })
                    .collect(),
            );
        }
        _ => {}
    }
    encoded
}

pub(super) fn decode(encoded: &serde_json::Value) -> Result<AnyValue, serde_json::Error> {
    if encoded.as_object().is_some_and(serde_json::Map::is_empty) {
        return Ok(AnyValue { value: None });
    }
    let value = if let Some(number) = encoded
        .get("doubleValue")
        .and_then(serde_json::Value::as_str)
    {
        Value::DoubleValue(match number {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            _ => return Err(serde_json::Error::custom("invalid protoJSON double")),
        })
    } else if let Some(array) = encoded.get("arrayValue") {
        let values = array.get("values").map_or(Ok(Vec::new()), |values| {
            values
                .as_array()
                .ok_or_else(|| serde_json::Error::custom("OTLP array values must be an array"))?
                .iter()
                .map(decode)
                .collect::<Result<Vec<_>, _>>()
        })?;
        Value::ArrayValue(ArrayValue { values })
    } else if let Some(list) = encoded.get("kvlistValue") {
        let values = list.get("values").map_or(Ok(Vec::new()), |values| {
            values
                .as_array()
                .ok_or_else(|| serde_json::Error::custom("OTLP key value list must be an array"))?
                .iter()
                .map(|entry| {
                    let mut raw = entry.clone();
                    let nested = raw
                        .as_object_mut()
                        .ok_or_else(|| {
                            serde_json::Error::custom("OTLP key value must be an object")
                        })?
                        .remove("value");
                    let mut kv: KeyValue = serde_json::from_value(raw)?;
                    kv.value = nested.as_ref().map(decode).transpose()?;
                    Ok(kv)
                })
                .collect::<Result<Vec<_>, serde_json::Error>>()
        })?;
        Value::KvlistValue(KeyValueList { values })
    } else {
        return AnyValue::deserialize(encoded.clone());
    };
    Ok(AnyValue { value: Some(value) })
}
