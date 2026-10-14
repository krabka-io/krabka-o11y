use super::{TopLevelQuerySplit, split_top_level_query};

pub(crate) fn split_top_level_arithmetic_query(query: &str) -> Option<TopLevelQuerySplit<'_>> {
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
