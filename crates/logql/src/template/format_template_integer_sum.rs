use super::template_integer_args;

pub(crate) fn format_template_integer_sum(args: &[String]) -> String {
    let Some(values) = template_integer_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .try_fold(0i64, i64::checked_add)
        .map_or_else(String::new, |value| value.to_string())
}
