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
    use super::metric_value_json;

    #[yare::parameterized(
        nan = { f64::NAN, "NaN" },
        positive_infinity = { f64::INFINITY, "Infinity" },
        negative_infinity = { f64::NEG_INFINITY, "-Infinity" },
    )]
    fn nonfinite_values_have_protojson_strings(value: f64, text: &str) {
        assert2::assert!(metric_value_json(value) == serde_json::json!(text));
    }

    #[test]
    fn finite_values_remain_json_numbers() {
        assert2::assert!(metric_value_json(0.0) == serde_json::json!(0.0));
    }
}
