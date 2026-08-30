pub(crate) fn has_word_boundary(query: &str, index: usize, len: usize) -> bool {
    query[..index]
        .chars()
        .next_back()
        .is_none_or(char::is_whitespace)
        && query[index + len..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
}
