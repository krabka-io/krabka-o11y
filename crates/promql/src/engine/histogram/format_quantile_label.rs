/// Quantile labels use `OpenMetrics` floats, including a decimal for integers.
pub(crate) fn format_quantile_label(value: f64) -> String {
    if !value.is_finite() {
        return crate::http_api::format_sample_value(value);
    }
    if value == 0.0 {
        return "0.0".to_owned();
    }
    if !(1e-4..1e6).contains(&value.abs()) {
        let text = format!("{value:e}");
        let (mantissa, exponent) = text
            .split_once('e')
            .expect("scientific float has an exponent");
        let exponent = exponent
            .parse::<i32>()
            .expect("float exponent is an integer");
        return format!("{mantissa}e{exponent:+03}");
    }
    let mut text = value.to_string();
    if !text.contains('.') {
        text.push_str(".0");
    }
    text
}

#[cfg(test)]
mod tests {
    #[test]
    fn quantile_labels_preserve_openmetrics_float_contract() {
        for (value, expected) in [
            (0.0, "0.0"),
            (-0.0, "0.0"),
            (1.0, "1.0"),
            (-1.0, "-1.0"),
            (0.25, "0.25"),
            (1e-6, "1e-06"),
            (1e6, "1e+06"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "+Inf"),
        ] {
            assert2::assert!(super::format_quantile_label(value) == expected);
        }
    }
}
