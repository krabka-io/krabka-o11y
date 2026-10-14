pub(crate) fn format_template_float(value: f64) -> String {
    if value == f64::INFINITY {
        return "+Inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_string();
    }
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value != 0.0 && (value.abs() < 0.0001 || value.abs() >= 1_000_000.0) {
        let text = format!("{value:e}");
        let exponent_separator = text.find('e').expect("scientific float has exponent");
        let mantissa = &text[..exponent_separator];
        let exponent: i32 = text[exponent_separator + 1..]
            .parse()
            .expect("float exponent fits in i32");
        return format!("{mantissa}e{exponent:+03}");
    }
    value.to_string()
}
