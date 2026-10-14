use super::{ScanResult, is_ident_continue};

pub(crate) fn scan_ident(s: &str, allow_dots: bool) -> ScanResult<String> {
    let mut end = 0;
    for entry in s.char_indices() {
        let idx = entry.0;
        let ch = entry.1;
        if is_ident_continue(ch) || (allow_dots && ch == '.') {
            end = idx + ch.len_utf8();
        } else {
            break;
        }
    }
    ScanResult {
        value: s[..end].to_string(),
        consumed: end,
    }
}
