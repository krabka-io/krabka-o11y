use super::*;

pub(crate) fn previous_char_boundary(value: &str, mut offset: usize) -> usize {
    while !value.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}
