use super::is_ident_continue;

pub(crate) fn scan_ident(s: &str, allow_dots: bool) -> (String, usize) {
    let mut end = 0;
    for (idx, ch) in s.char_indices() {
        if is_ident_continue(ch) || (allow_dots && ch == '.') {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    (s[..end].to_string(), end)
}
