use super::{Result, PromqlError};

pub(crate) fn matching_delimiter(chars: &[char], start: usize, open: char, close: char) -> Result<usize> {
    let mut depth = 0_i32;
    let mut quote = None;
    let mut index = start;
    while index < chars.len() {
        let ch = chars[index];
        if let Some(quote_ch) = quote {
            if ch == '\\' {
                index += 2;
                continue;
            }
            if ch == quote_ch {
                quote = None;
            }
            index += 1;
            continue;
        }
        match ch {
            '"' | '\'' | '`' => quote = Some(ch),
            _ if ch == open => depth += 1,
            _ if ch == close => {
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    Err(PromqlError::Parse(format!("unclosed `{open}` in query")))
}
