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
        let exponent_at = text.find('e').expect("scientific float has an exponent");
        let mantissa = &text[..exponent_at];
        let exponent = &text[exponent_at + 1..];
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
    #[yare::parameterized(
        zero = { 0.0, "0.0" },
        negative_zero = { -0.0, "0.0" },
        one = { 1.0, "1.0" },
        negative_one = { -1.0, "-1.0" },
        fraction = { 0.25, "0.25" },
        small_scientific = { 1e-6, "1e-06" },
        large_scientific = { 1e6, "1e+06" },
        nan = { f64::NAN, "NaN" },
        infinity = { f64::INFINITY, "+Inf" },
    )]
    fn quantile_labels_preserve_openmetrics_float_contract(value: f64, expected: &str) {
        assert2::assert!(crate::engine::histogram::format_quantile_label(value) == expected);
    }
}
