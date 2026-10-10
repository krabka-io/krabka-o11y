use super::{has_word_boundary, split_top_level_query};

pub(crate) fn split_top_level_set_query(query: &str) -> Option<(&str, &'static str, &str)> {
    split_top_level_query(query, |index, ch| {
        if !ch.is_ascii_alphabetic() {
            return None;
        }
        ["unless", "and", "or"].into_iter().find(|operator| {
            query[index..].starts_with(operator) && has_word_boundary(query, index, operator.len())
        })
    })
}
