use super::{
    ParseError, ParsedTemplateToken, template_parse_error,
    template_quote_scanner::TemplateQuoteScanner,
};

pub(crate) fn parse_template_parenthesized_token(
    command: &str,
    start: usize,
) -> Result<ParsedTemplateToken, ParseError> {
    let mut depth = 0usize;
    let mut quotes = TemplateQuoteScanner::default();
    let mut index = start;
    while index < command.len() {
        let ch = command[index..]
            .chars()
            .next()
            .expect("index is within command");
        if quotes.consume_quoted(ch) {
            index += ch.len_utf8();
            continue;
        }
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
                if depth == 0 {
                    return Ok(ParsedTemplateToken {
                        token: command[start..=index].to_string(),
                        next_position: index.saturating_add(ch.len_utf8()),
                    });
                }
            }
            _ => {}
        }
        index += ch.len_utf8();
    }
    Err(template_parse_error("unterminated template parenthesis"))
}
