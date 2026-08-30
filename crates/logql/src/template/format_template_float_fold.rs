use super::{template_float_args, format_template_float};

pub(crate) fn format_template_float_fold(args: &[String], op: impl Fn(f64, f64) -> Option<f64>) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return String::new();
    };
    values
        .try_fold(first, op)
        .filter(|value| value.is_finite())
        .map_or_else(String::new, format_template_float)
}
