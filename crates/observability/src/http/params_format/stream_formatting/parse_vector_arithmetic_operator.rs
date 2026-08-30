use super::*;

pub(crate) fn parse_vector_arithmetic_operator(
    query: &str,
    position: usize,
) -> Option<(&'static str, usize)> {
    for (raw, formatted) in [
        ("+", "+"),
        ("-", "-"),
        ("*", "*"),
        ("/", "/"),
        ("%", "%"),
        ("^", "^"),
    ] {
        if query[position..].starts_with(raw) {
            return Some((formatted, position + raw.len()));
        }
    }
    None
}
