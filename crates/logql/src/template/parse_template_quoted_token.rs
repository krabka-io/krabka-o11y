use super::{ParseError, ParsedTemplateToken, template_parse_error};

pub(crate) fn parse_template_quoted_token(
    command: &str,
    start: usize,
    quote: char,
) -> Result<ParsedTemplateToken, ParseError> {
    let mut escaped = false;
    let value_start = start.saturating_add(quote.len_utf8());
    let mut index = value_start;
    while index < command.len() {
        let ch = command[index..]
            .chars()
            .next()
            .expect("index is within command");
        if escaped {
            escaped = false;
            index += ch.len_utf8();
            continue;
        }
        if matches!(quote, '"' | '\'') && ch == '\\' {
            escaped = true;
            index += ch.len_utf8();
            continue;
        }
        if ch == quote {
            return Ok(ParsedTemplateToken {
                token: command[start..=index].to_string(),
                next_position: index.saturating_add(quote.len_utf8()),
            });
        }
        index += ch.len_utf8();
    }
    Err(template_parse_error("unterminated template string"))
}
