use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, any_value::Value};

use super::{Deserialize, Serialize};
mod array_value;
mod float_value;
mod wire_json;

/// A typed attribute value. Block encoding preserves arrays.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Double(#[serde(with = "float_value")] f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Array(#[serde(with = "array_value")] Vec<AttrValue>),
    /// Lossless OTLP `AnyValue` JSON for types excluded from typed `TraceQL` operands.
    Unsupported(String),
}

impl From<&AnyValue> for AttrValue {
    fn from(value: &AnyValue) -> Self {
        match &value.value {
            Some(Value::StringValue(value)) => Self::Str(value.clone()),
            Some(Value::StringValueStrindex(value)) => Self::Str(format!("strindex:{value}")),
            Some(Value::IntValue(value)) => Self::Int(*value),
            Some(Value::DoubleValue(value)) => Self::Double(*value),
            Some(Value::BoolValue(value)) => Self::Bool(*value),
            Some(Value::BytesValue(value)) => Self::Bytes(value.clone()),
            Some(Value::ArrayValue(value)) => {
                Self::Array(value.values.iter().map(Self::from).collect())
            }
            Some(Value::KvlistValue(_)) | None => {
                Self::Unsupported(Self::encode_otlp_json(value).to_string())
            }
        }
    }
}

impl AttrValue {
    /// Restores the OTLP wire value, including opaque types and array shape.
    /// # Panics
    /// Panics if an opaque payload was constructed with invalid OTLP JSON.
    pub fn otlp_value(&self) -> AnyValue {
        let value = match self {
            Self::Str(value) => Value::StringValue(value.clone()),
            Self::Int(value) => Value::IntValue(*value),
            Self::Double(value) => Value::DoubleValue(*value),
            Self::Bool(value) => Value::BoolValue(*value),
            Self::Bytes(value) => Value::BytesValue(value.clone()),
            Self::Array(values) => Value::ArrayValue(ArrayValue {
                values: values.iter().map(Self::otlp_value).collect(),
            }),
            Self::Unsupported(value) => {
                return Self::parse_otlp_json(
                    &serde_json::from_str(value).expect("opaque payload is valid JSON"),
                )
                .expect("unsupported attributes retain valid OTLP AnyValue JSON");
            }
        };
        AnyValue { value: Some(value) }
    }
    /// Restores lossless protoJSON, including non-finite numbers in opaque types.
    #[must_use]
    pub fn otlp_json(&self) -> serde_json::Value {
        Self::encode_otlp_json(&self.otlp_value())
    }

    /// Converts the value to a typed `TraceQL` operand.
    ///
    /// A heterogeneous array, or one holding arrays, bytes, or opaque values,
    /// becomes one opaque value of its lossless OTLP JSON, as do bytes.
    #[must_use]
    pub fn traceql_value(&self) -> Option<krabka_traceql::AttrValue> {
        if let Self::Array(values) = self
            && values.iter().any(|element| {
                matches!(
                    element,
                    Self::Array(_) | Self::Bytes(_) | Self::Unsupported(_)
                ) || values.first().is_some_and(|first| {
                    std::mem::discriminant(first) != std::mem::discriminant(element)
                })
            })
        {
            return Some(krabka_traceql::AttrValue::Unsupported(
                self.otlp_json().to_string(),
            ));
        }
        Some(match self {
            Self::Unsupported(value) => krabka_traceql::AttrValue::Unsupported(value.clone()),
            Self::Array(values) => krabka_traceql::AttrValue::Array(
                values
                    .iter()
                    .map(Self::traceql_value)
                    .collect::<Option<Vec<_>>>()?,
            ),
            Self::Str(value) => krabka_traceql::AttrValue::Str(value.clone()),
            Self::Int(value) => krabka_traceql::AttrValue::Int(*value),
            Self::Double(value) => krabka_traceql::AttrValue::Float(*value),
            Self::Bool(value) => krabka_traceql::AttrValue::Bool(*value),
            Self::Bytes(_) => krabka_traceql::AttrValue::Unsupported(self.otlp_json().to_string()),
        })
    }

    /// Serializes an OTLP wire value using the protobuf JSON double convention.
    #[must_use]
    pub fn encode_otlp_json(value: &AnyValue) -> serde_json::Value {
        wire_json::encode(value)
    }

    /// Decodes protobuf JSON, including special doubles nested in arrays or maps.
    /// # Errors
    /// Returns an error for invalid OTLP JSON values.
    pub fn parse_otlp_json(value: &serde_json::Value) -> Result<AnyValue, serde_json::Error> {
        wire_json::decode(value)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use opentelemetry_proto::tonic::common::v1::{KeyValue, KeyValueList};

    use super::{AnyValue, ArrayValue, AttrValue, Value};

    #[test]
    fn empty_anyvalue_retains_its_position_in_arrays_and_opaque_lists() {
        for json in [
            serde_json::json!({}),
            serde_json::json!({"arrayValue":{"values":[{}, {"intValue":"7"}, {}]}}),
            serde_json::json!({"kvlistValue":{"values":[{"key":"empty","value":{}}]}}),
        ] {
            let wire = AttrValue::parse_otlp_json(&json).unwrap();
            check!(AttrValue::encode_otlp_json(&wire) == json);
            let opaque = AttrValue::Unsupported(json.to_string());
            check!(opaque.otlp_json() == json);
        }
        check!(AttrValue::parse_otlp_json(&serde_json::json!(null)).is_err());
    }

    #[test]
    fn nonfinite_values_survive_native_json_and_recursive_otlp_payloads() {
        let model = AttrValue::Array(vec![
            AttrValue::Double(f64::NAN),
            AttrValue::Double(f64::INFINITY),
            AttrValue::Double(f64::NEG_INFINITY),
            AttrValue::Double(-0.0),
        ]);
        let native = serde_json::to_string(&model).unwrap();
        let decoded: AttrValue = serde_json::from_str(&native).unwrap();
        let AttrValue::Array(values) = decoded else {
            panic!("expected array")
        };
        for (value, expected) in
            values
                .iter()
                .zip([f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0])
        {
            let AttrValue::Double(actual) = value else {
                panic!("expected double")
            };
            check!(actual.to_bits() == expected.to_bits());
        }
        let wire = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "nested".into(),
                    value: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue {
                                    value: Some(Value::DoubleValue(f64::NAN)),
                                },
                                AnyValue {
                                    value: Some(Value::DoubleValue(f64::INFINITY)),
                                },
                                AnyValue {
                                    value: Some(Value::DoubleValue(f64::NEG_INFINITY)),
                                },
                                AnyValue {
                                    value: Some(Value::IntValue(i64::MAX)),
                                },
                            ],
                        })),
                    }),
                    ..KeyValue::default()
                }],
            })),
        };
        let json = AttrValue::encode_otlp_json(&wire);
        check!(
            json == serde_json::json!({"kvlistValue":{"values":[{"key":"nested","value":{"arrayValue":{"values":[{"doubleValue":"NaN"},{"doubleValue":"Infinity"},{"doubleValue":"-Infinity"},{"intValue":"9223372036854775807"}]}}}]}})
        );
        let opaque = AttrValue::Unsupported(json.to_string());
        check!(opaque.otlp_json() == json);
        let reconstructed = AttrValue::parse_otlp_json(&json).unwrap();
        check!(AttrValue::encode_otlp_json(&reconstructed) == json);
        for invalid in [
            serde_json::json!({"doubleValue":"nan"}),
            serde_json::json!({"arrayValue":{"values":null}}),
        ] {
            check!(AttrValue::parse_otlp_json(&invalid).is_err());
        }
    }
}
