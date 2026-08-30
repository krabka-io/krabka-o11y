use super::pattern_value_is_variable;

/// Templatizes the variable whitespace-delimited tokens inside a free-text
/// value, for example an embedded request id or timestamp in a `message`
/// field. It leaves the constant words intact, so distinct messages stay
/// distinct patterns.
pub(crate) fn templatize_text(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            if pattern_value_is_variable(token) {
                "<_>".to_string()
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
