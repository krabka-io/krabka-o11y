use super::{ParseError, template_parse_error, is_wrapped_template_token};

pub(crate) fn ensure_template_quoted_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
    quote: char,
) -> Result<(), ParseError> {
    if next <= pos {
        return Err(template_parse_error(
            "template token parser did not advance",
        ));
    }
    if next > command.len() {
        return Err(template_parse_error(
            "template token parser advanced past command",
        ));
    }
    if !is_wrapped_template_token(token, quote) {
        return Err(template_parse_error(
            "template token parser returned unwrapped quoted token",
        ));
    }
    Ok(())
}
