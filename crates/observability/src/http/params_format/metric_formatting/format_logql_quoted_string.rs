pub(crate) fn format_logql_quoted_string(value: &str) -> String {
    let mut formatted = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => formatted.push_str("\\\\"),
            '"' => formatted.push_str("\\\""),
            '\n' => formatted.push_str("\\n"),
            '\r' => formatted.push_str("\\r"),
            '\t' => formatted.push_str("\\t"),
            other => formatted.push(other),
        }
    }
    formatted.push('"');
    formatted
}
