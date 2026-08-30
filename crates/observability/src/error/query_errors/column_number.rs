use super::*;

pub(crate) fn column_number(query: &str, position: usize) -> usize {
    let prefix = &query[..position.min(query.len())];
    prefix
        .rsplit_once('\n')
        .map_or(prefix, |(_, line)| line)
        .chars()
        .count()
        + 1
}
