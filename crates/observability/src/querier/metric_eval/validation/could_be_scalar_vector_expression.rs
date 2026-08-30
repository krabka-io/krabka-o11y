use super::*;

pub(crate) fn could_be_scalar_vector_expression(query: &str) -> bool {
    let trimmed = query.trim_start();
    let Some(first) = trimmed.chars().next() else {
        return false;
    };
    if first.is_ascii_digit() || matches!(first, '+' | '-' | '.' | '(') {
        return true;
    }
    // `== '_'` against `!= '_'` is a permanent survivor. The branch it guards
    // returns true only for three literal identifiers: a leading `_` cannot
    // begin any of them, and every other character the mutation newly admits
    // takes an empty identifier, which matches none of them.
    if first.is_ascii_alphabetic() || first == '_' {
        let ident_len = trimmed
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(char::len_utf8)
            .sum::<usize>();
        return matches!(
            &trimmed[..ident_len],
            "vector" | "label_replace" | "label_join" | "sort" | "sort_desc"
        );
    }
    false
}
