pub(crate) fn column_number(query: &str, position: usize) -> usize {
    let prefix = &query[..position.min(query.len())];
    prefix
        .rfind('\n')
        .map_or(prefix, |index| &prefix[index + 1..])
        .chars()
        .count()
        + 1
}
