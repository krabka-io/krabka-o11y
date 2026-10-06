use super::{ParseError, template_parse_error};

pub(crate) fn split_template_pipeline(expression: &str) -> Result<Vec<&str>, ParseError> {
    let mut commands = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut depth = 0usize;
    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if matches!(quote, Some('"' | '\'')) && ch == '\\' {
            escaped = true;
            continue;
        }
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '"' | '\'' | '`') {
            quote = Some(ch);
        } else if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth = depth
                .checked_sub(1)
                .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
        } else if ch == '|' && depth == 0 {
            let command = expression[start..index].trim();
            if command.is_empty() {
                return Err(template_parse_error("expected template command"));
            }
            commands.push(command);
            start = index + ch.len_utf8();
        }
    }
    if quote.is_some() {
        return Err(template_parse_error("unterminated template string"));
    }
    if depth != 0 {
        return Err(template_parse_error("unterminated template parenthesis"));
    }
    let command = expression[start..].trim();
    if command.is_empty() {
        return Err(template_parse_error("expected template command"));
    }
    commands.push(command);
    Ok(commands)
}
