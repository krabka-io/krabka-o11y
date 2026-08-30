pub(crate) fn template_integer_args(args: &[String]) -> Option<Vec<i64>> {
    args.iter().map(|value| value.parse::<i64>().ok()).collect()
}
