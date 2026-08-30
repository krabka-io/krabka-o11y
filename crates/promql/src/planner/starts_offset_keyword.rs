use super::is_ident_char;

pub(crate) fn starts_offset_keyword(chars: &[char], index: usize) -> bool {
    const OFFSET: &str = "offset";
    if index + OFFSET.len() > chars.len() {
        return false;
    }
    let word = chars[index..index + OFFSET.len()]
        .iter()
        .collect::<String>();
    if !word.eq_ignore_ascii_case(OFFSET) {
        return false;
    }
    let before_ok = index == 0 || !is_ident_char(chars[index - 1]);
    let after = index + OFFSET.len();
    let after_ok = after == chars.len() || !is_ident_char(chars[after]);
    before_ok && after_ok
}
