use super::{Result, ScanResult, TraceqlError};

pub(crate) fn scan_string(s: &str) -> Result<ScanResult<String>> {
    let mut out = String::new();
    let mut escaped = false;
    for entry in s.char_indices().skip(1) {
        let idx = entry.0;
        let ch = entry.1;
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
            return Ok(ScanResult {
                value: out,
                consumed: idx + ch.len_utf8(),
            });
        } else {
            out.push(ch);
        }
    }
    Err(TraceqlError::Parse("unterminated string literal".into()))
}
