use super::{ParseError, syntax_error};

pub(crate) fn scan_top_level(input: &str, mut found: impl FnMut(usize)) -> Result<(), ParseError> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (at, ch) in input.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' && q == '"' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| syntax_error("unmatched closing delimiter"))?;
            }
            _ if depth == 0 => found(at),
            _ => {}
        }
    }
    if quote.is_some() || depth != 0 {
        return Err(syntax_error("unterminated expression"));
    }
    Ok(())
}
