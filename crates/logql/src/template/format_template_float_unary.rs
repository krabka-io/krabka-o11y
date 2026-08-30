use super::format_template_float;

pub(crate) fn format_template_float_unary(value: &str, op: impl FnOnce(f64) -> f64) -> String {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(op)
        .filter(|value| value.is_finite())
        .map_or_else(String::new, format_template_float)
}
