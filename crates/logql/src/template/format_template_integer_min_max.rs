use super::template_integer_args;

pub(crate) fn format_template_integer_min_max(
    args: &[String],
    op: impl Fn(i64, i64) -> i64,
) -> String {
    let Some(values) = template_integer_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .reduce(op)
        .map_or_else(String::new, |value| value.to_string())
}
