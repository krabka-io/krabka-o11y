use super::{
    ParseError, ParsedTemplateAction, skip_leading_template_whitespace, template_action_trim_left,
    template_action_trim_right, template_parse_error,
};

pub(crate) fn parse_template_action(
    template: &str,
    open: usize,
) -> Result<ParsedTemplateAction<'_>, ParseError> {
    let mut expression_start = open + 2;
    let trim_left = template_action_trim_left(template, open)?;
    if trim_left {
        expression_start += 1;
    }
    let close_offset = action_end(&template[expression_start..])
        .ok_or_else(|| template_parse_error("expected closing template action"))?;
    let close = expression_start + close_offset;
    let trim_right = template_action_trim_right(template, expression_start, close);
    let expression_end = if trim_right { close - 1 } else { close };
    let untrimmed_next_pos = close + 2;
    let mut next_pos = untrimmed_next_pos;
    if trim_right {
        next_pos = skip_leading_template_whitespace(template, next_pos);
        if next_pos < untrimmed_next_pos {
            return Err(template_parse_error(
                "template action parser did not advance",
            ));
        }
    }
    Ok(ParsedTemplateAction {
        expression: template[expression_start..expression_end].trim(),
        next_pos,
        trim_left,
    })
}

fn action_end(value: &str) -> Option<usize> {
    if value.trim_start().starts_with("/*") {
        let end = value.find("*/")? + 2;
        return value[end..].find("}}").map(|offset| end + offset);
    }
    let mut quote = None;
    let mut escaped = false;
    for (offset, ch) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if let Some(delimiter) = quote {
            if delimiter != '`' && ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                quote = None;
            }
        } else if matches!(ch, '\"' | '\'' | '`') {
            quote = Some(ch);
        } else if value[offset..].starts_with("}}") {
            return Some(offset);
        }
    }
    None
}
