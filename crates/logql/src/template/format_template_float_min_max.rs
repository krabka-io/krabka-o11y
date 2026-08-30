use super::*;

pub(crate) fn format_template_float_min_max(args: &[String], op: impl Fn(f64, f64) -> f64) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .reduce(op)
        .map_or_else(String::new, format_template_float)
}
