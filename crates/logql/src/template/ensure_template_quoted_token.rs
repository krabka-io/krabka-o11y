use super::{ParseError, ensure_template_token, is_wrapped_template_token, template_parse_error};

pub(crate) fn ensure_template_quoted_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
    quote: char,
) -> Result<(), ParseError> {
    ensure_template_token(command, pos, token, next, |token| {
        if is_wrapped_template_token(token, quote) {
            Ok(())
        } else {
            Err(template_parse_error(
                "template token parser returned unwrapped quoted token",
            ))
        }
    })
}
