use serde_json::{Value, json};

pub(crate) fn metric_value_json(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value == f64::INFINITY {
        json!("Infinity")
    } else if value == f64::NEG_INFINITY {
        json!("-Infinity")
    } else {
        json!(value)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn nonfinite_values_have_protojson_strings() {
        for (value, text) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert2::assert!(super::metric_value_json(value) == serde_json::json!(text));
        }
        assert2::assert!(super::metric_value_json(0.0) == serde_json::json!(0.0));
    }
}
