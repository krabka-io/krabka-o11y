use super::{format_template_float, template_float_args};

pub(crate) fn format_template_float_min_max(
    args: &[String],
    op: impl Fn(f64, f64) -> f64,
) -> String {
    let values = template_float_args(args);
    values
        .into_iter()
        .reduce(|left, right| {
            if left.is_nan() || right.is_nan() {
                f64::NAN
            } else {
                op(left, right)
            }
        })
        .map_or_else(String::new, format_template_float)
}
