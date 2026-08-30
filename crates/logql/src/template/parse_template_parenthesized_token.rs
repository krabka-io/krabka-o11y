use super::*;

pub(crate) fn parse_template_parenthesized_token(
    command: &str,
    start: usize,
) -> Result<(String, usize), ParseError> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (offset, ch) in command[start..].char_indices() {
        let index = start + offset;
        if escaped {
            escaped = false;
            continue;
        }
        if quote == Some('"') && ch == '\\' {
            escaped = true;
            continue;
        }
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '"' | '`') {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
                if depth == 0 {
                    return Ok((
                        command[start..=index].to_string(),
                        index.saturating_add(ch.len_utf8()),
                    ));
                }
            }
            _ => {}
        }
    }
    Err(template_parse_error("unterminated template parenthesis"))
}
