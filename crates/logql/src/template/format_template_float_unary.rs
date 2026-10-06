use super::format_template_float;

pub(crate) fn format_template_float_unary(value: &str, op: impl FnOnce(f64) -> f64) -> String {
    format_template_float(op(value.parse::<f64>().unwrap_or_default()))
}
