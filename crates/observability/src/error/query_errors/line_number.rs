use super::*;

pub(crate) fn line_number(query: &str, position: usize) -> usize {
    query[..position.min(query.len())]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}
