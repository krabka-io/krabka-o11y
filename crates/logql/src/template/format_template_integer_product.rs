use super::template_integer_args;

pub(crate) fn format_template_integer_product(args: &[String]) -> String {
    let Some(values) = template_integer_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .try_fold(1i64, i64::checked_mul)
        .map_or_else(String::new, |value| value.to_string())
}
