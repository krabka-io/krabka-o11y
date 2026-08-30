
pub(crate) fn format_template_integer_binary(
    args: &[String],
    op: impl FnOnce(i64, i64) -> Option<i64>,
) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let (Ok(left), Ok(right)) = (args[0].parse::<i64>(), args[1].parse::<i64>()) else {
        return String::new();
    };
    op(left, right).map_or_else(String::new, |value| value.to_string())
}
