pub(crate) fn parse_template_integer(value: &str) -> String {
    value
        .parse::<i64>()
        .map_or_else(|_| String::new(), |value| value.to_string())
}
