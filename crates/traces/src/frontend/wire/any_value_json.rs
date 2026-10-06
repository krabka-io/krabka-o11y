use super::{ArrayValueJson, AttrValue, Deserialize, Serialize};

/// OTLP `AnyValue`, holding the variants `TraceQL` surfaces.
///
/// Tempo emits `intValue` as a string and groups multi-valued attributes under
/// `arrayValue`. That matches the querier's `attr_value_json` and
/// `attr_values_json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AnyValueJson {
    #[serde(rename = "stringValue")]
    StringValue(String),
    #[serde(rename = "intValue")]
    IntValue(String),
    #[serde(rename = "doubleValue")]
    DoubleValue(#[serde(with = "double_json")] f64),
    #[serde(rename = "boolValue")]
    BoolValue(bool),
    #[serde(rename = "arrayValue")]
    ArrayValue(ArrayValueJson),
    #[serde(untagged)]
    Unsupported(serde_json::Value),
}

impl From<&AttrValue> for AnyValueJson {
    fn from(v: &AttrValue) -> Self {
        match v {
            AttrValue::Unsupported(value) => AnyValueJson::Unsupported(
                serde_json::from_str(value).expect("opaque attribute contains AnyValue JSON"),
            ),
            AttrValue::Array(values) => AnyValueJson::ArrayValue(ArrayValueJson {
                values: values.iter().map(AnyValueJson::from).collect(),
            }),
            AttrValue::Str(s) => AnyValueJson::StringValue(s.clone()),
            AttrValue::Int(i) => AnyValueJson::IntValue(i.to_string()),
            AttrValue::Float(f) => AnyValueJson::DoubleValue(*f),
            AttrValue::Bool(b) => AnyValueJson::BoolValue(*b),
        }
    }
}

mod double_json {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S: Serializer>(
        value: impl std::borrow::Borrow<f64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let value = *value.borrow();
        if value.is_nan() {
            serializer.serialize_str("NaN")
        } else if value.is_infinite() {
            serializer.serialize_str(if value.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            })
        } else {
            serializer.serialize_f64(value)
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(value) => value
                .as_f64()
                .ok_or_else(|| D::Error::custom("invalid double")),
            serde_json::Value::String(value) => match value.as_str() {
                "NaN" => Ok(f64::NAN),
                "Infinity" => Ok(f64::INFINITY),
                "-Infinity" => Ok(f64::NEG_INFINITY),
                _ => Err(D::Error::custom("invalid protoJSON double token")),
            },
            _ => Err(D::Error::custom("invalid protoJSON double")),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{AnyValueJson, ArrayValueJson};

    #[test]
    fn nonfinite_doubles_roundtrip_through_scalar_and_array_frontend_values() {
        for (number, text) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            let scalar = AnyValueJson::DoubleValue(number);
            let encoded = serde_json::to_value(&scalar).unwrap();
            assert!(encoded == serde_json::json!({"doubleValue":text}));
            let decoded: AnyValueJson = serde_json::from_value(encoded).unwrap();
            let AnyValueJson::DoubleValue(actual) = decoded else {
                panic!("double discriminator lost")
            };
            assert!(actual.to_bits() == number.to_bits() || actual.is_nan() && number.is_nan());
            let array = AnyValueJson::ArrayValue(ArrayValueJson {
                values: vec![scalar],
            });
            let encoded = serde_json::to_value(&array).unwrap();
            let decoded: AnyValueJson = serde_json::from_value(encoded.clone()).unwrap();
            assert!(serde_json::to_value(decoded).unwrap() == encoded);
        }
    }
}
