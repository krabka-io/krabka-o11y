use super::split_top_level_query;

pub(crate) fn split_top_level_comparison_query(query: &str) -> Option<(&str, &'static str, &str)> {
    split_top_level_query(query, |index, ch| {
        if !matches!(ch, '>' | '<' | '=' | '!') {
            return None;
        }
        [">=", "<=", "==", "!=", ">", "<"]
            .into_iter()
            .find(|operator| query[index..].starts_with(operator))
    })
}
