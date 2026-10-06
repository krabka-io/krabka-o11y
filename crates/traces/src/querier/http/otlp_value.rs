use super::{AttrValue, OtlpAnyValue, OtlpValue};

pub(crate) fn otlp_value(value: &AttrValue) -> OtlpAnyValue {
    OtlpAnyValue {
        value: Some(match value {
            AttrValue::Unsupported(value) => {
                return crate::span::AttrValue::parse_otlp_json(
                    &serde_json::from_str(value).expect("opaque attribute contains AnyValue JSON"),
                )
                .expect("opaque attribute contains valid OTLP AnyValue JSON");
            }
            AttrValue::Array(values) => OtlpValue::ArrayValue(super::OtlpArrayValue {
                values: values.iter().map(otlp_value).collect(),
            }),
            AttrValue::Str(value) => OtlpValue::StringValue(value.clone()),
            AttrValue::Int(value) => OtlpValue::IntValue(*value),
            AttrValue::Float(value) => OtlpValue::DoubleValue(*value),
            AttrValue::Bool(value) => OtlpValue::BoolValue(*value),
        }),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{AttrValue, OtlpValue, otlp_value};

    #[test]
    fn opaque_mixed_array_retains_nonfinite_and_large_integer_values() {
        let value = AttrValue::Unsupported(r#"{"arrayValue":{"values":[{"doubleValue":"Infinity"},{"intValue":"9223372036854775807"},{"doubleValue":"NaN"}]}}"#.into());
        let Some(OtlpValue::ArrayValue(array)) = otlp_value(&value).value else {
            panic!("array shape lost")
        };
        assert!(array.values.len() == 3);
        assert!(array.values[0].value == Some(OtlpValue::DoubleValue(f64::INFINITY)));
        assert!(array.values[1].value == Some(OtlpValue::IntValue(i64::MAX)));
        let Some(OtlpValue::DoubleValue(value)) = array.values[2].value.as_ref() else {
            panic!("double type lost")
        };
        assert!(value.is_nan());
    }
}
