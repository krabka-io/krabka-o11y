use super::{AttrValue, Value, json};

pub(crate) fn attr_value_json(value: &AttrValue) -> Value {
    match value {
        AttrValue::Unsupported(value) => {
            serde_json::from_str(value).expect("opaque attribute contains AnyValue JSON")
        }
        AttrValue::Array(values) => {
            json!({"arrayValue":{"values":values.iter().map(attr_value_json).collect::<Vec<_>>()}})
        }
        AttrValue::Str(v) => json!({"stringValue": v}),
        AttrValue::Int(v) => json!({"intValue": v.to_string()}),
        AttrValue::Float(v) => json!({"doubleValue": super::metric_value_json(*v)}),
        AttrValue::Bool(v) => json!({"boolValue": v}),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{AttrValue, attr_value_json, json};

    #[test]
    fn nonfinite_scalar_and_array_values_keep_protojson_tokens() {
        for (number, text) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert!(attr_value_json(&AttrValue::Float(number)) == json!({"doubleValue":text}));
            assert!(
                attr_value_json(&AttrValue::Array(vec![AttrValue::Float(number)]))
                    == json!({"arrayValue":{"values":[{"doubleValue":text}]}})
            );
        }
    }
}
