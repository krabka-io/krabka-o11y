use super::template_integer_args;

pub(crate) fn format_template_integer_product(args: &[String]) -> String {
    let values = template_integer_args(args);
    values.into_iter().fold(1i64, i64::wrapping_mul).to_string()
}
