use super::template_integer_args;

pub(crate) fn format_template_integer_sum(args: &[String]) -> String {
    let values = template_integer_args(args);
    values.into_iter().fold(0i64, i64::wrapping_add).to_string()
}
