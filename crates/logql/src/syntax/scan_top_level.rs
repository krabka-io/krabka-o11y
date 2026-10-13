use super::{ParseError, syntax_error};

pub(crate) fn scan_top_level(input: &str, mut found: impl FnMut(usize)) -> Result<(), ParseError> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    let mut at = 0;
    while at < input.len() {
        let ch = input[at..].chars().next().expect("index is within input");
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' && q == '"' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            at += ch.len_utf8();
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
        at += ch.len_utf8();
    }
    if quote.is_some() || depth != 0 {
        return Err(syntax_error("unterminated expression"));
    }
    Ok(())
}
