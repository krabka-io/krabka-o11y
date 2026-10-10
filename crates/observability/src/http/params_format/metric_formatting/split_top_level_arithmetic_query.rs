use super::split_top_level_query;

pub(crate) fn split_top_level_arithmetic_query(query: &str) -> Option<(&str, &'static str, &str)> {
    split_top_level_query(query, |_, ch| match ch {
        '+' => Some("+"),
        '-' => Some("-"),
        '*' => Some("*"),
        '/' => Some("/"),
        '%' => Some("%"),
        '^' => Some("^"),
        _ => None,
    })
}
