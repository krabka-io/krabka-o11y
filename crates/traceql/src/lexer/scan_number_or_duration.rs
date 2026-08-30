use super::{Result, Token, TraceqlError, is_ident_continue};

pub(crate) fn scan_number_or_duration(s: &str) -> Result<(Token, usize)> {
    let mut end = 0;
    let mut has_dot = false;
    let mut chars = s.char_indices().peekable();
    while let Some((idx, ch)) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            end = idx + ch.len_utf8();
            chars.next();
        } else if ch == '.'
            && !has_dot
            && chars
                .clone()
                .nth(1)
                .is_some_and(|(_, next)| next.is_ascii_digit())
        {
            has_dot = true;
            chars.next();
        } else {
            break;
        }
    }

    let mut ident_end = end;
    for (idx, ch) in s[end..].char_indices() {
        if is_ident_continue(ch) || ch == 'µ' {
            ident_end = end + idx + ch.len_utf8();
        } else {
            break;
        }
    }
    if ident_end > end {
        return Ok((Token::Ident(s[..ident_end].to_string()), ident_end));
    }

    if has_dot {
        let v = s[..end]
            .parse::<f64>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        Ok((Token::Float(v), end))
    } else {
        let v = s[..end]
            .parse::<i64>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        Ok((Token::Int(v), end))
    }
}
