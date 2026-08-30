use super::{Result, TraceqlError};

pub(crate) fn scan_string(s: &str) -> Result<(String, usize)> {
    let mut out = String::new();
    let mut escaped = false;
    for (idx, ch) in s.char_indices().skip(1) {
        if escaped {
            out.push(match ch {
                '"' => '"',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Ok((out, idx + ch.len_utf8()));
        } else {
            out.push(ch);
        }
    }
    Err(TraceqlError::Parse("unterminated string literal".into()))
}
