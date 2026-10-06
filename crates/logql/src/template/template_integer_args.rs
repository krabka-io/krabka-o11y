use super::parse_template_integer;

pub(crate) fn template_integer_args(args: &[String]) -> Vec<i64> {
    args.iter()
        .map(|value| parse_template_integer(value))
        .collect()
}
