use super::is_ident_char;

pub(crate) fn consume_ident(chars: &[char], mut index: usize) -> usize {
    while chars.get(index).is_some_and(|ch| is_ident_char(*ch)) {
        index += 1;
    }
    index
}
