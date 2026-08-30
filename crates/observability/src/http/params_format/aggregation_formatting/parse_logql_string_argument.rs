pub(crate) fn parse_logql_string_argument(argument: &str) -> Option<String> {
    if let Some(inner) = argument
        .strip_prefix('`')
        .and_then(|argument| argument.strip_suffix('`'))
    {
        return Some(inner.to_string());
    }

    let inner = argument
        .strip_prefix('"')
        .and_then(|argument| argument.strip_suffix('"'))?;
    let mut parsed = String::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            parsed.push(match chars.next()? {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                other => other,
            });
        } else {
            parsed.push(ch);
        }
    }
    Some(parsed)
}
