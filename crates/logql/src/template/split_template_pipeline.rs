use super::{ParseError, template_parse_error};

pub(crate) fn split_template_pipeline(expression: &str) -> Result<Vec<&str>, ParseError> {
    let mut commands = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in expression.char_indices() {
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
        } else if ch == '|' {
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
    let command = expression[start..].trim();
    if !command.is_empty() {
        commands.push(command);
    }
    Ok(commands)
}
