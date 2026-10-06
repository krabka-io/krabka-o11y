use super::parse_template_integer;

pub(crate) fn format_template_integer_binary(
    args: &[String],
    op: impl FnOnce(i64, i64) -> Option<i64>,
) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let left = parse_template_integer(&args[0]);
    let right = parse_template_integer(&args[1]);
    op(left, right).map_or_else(String::new, |value| value.to_string())
}
