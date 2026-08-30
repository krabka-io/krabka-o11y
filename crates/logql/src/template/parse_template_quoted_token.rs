use super::*;

pub(crate) fn parse_template_quoted_token(
    command: &str,
    start: usize,
    quote: char,
) -> Result<(String, usize), ParseError> {
    let mut escaped = false;
    let value_start = start.saturating_add(quote.len_utf8());
    for (offset, ch) in command[value_start..].char_indices() {
        let index = value_start.saturating_add(offset);
        if escaped {
            escaped = false;
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Ok((
                command[start..=index].to_string(),
                index.saturating_add(quote.len_utf8()),
            ));
        }
    }
    Err(template_parse_error("unterminated template string"))
}
