use super::{ParseError, ensure_template_token, template_parse_error};

pub(crate) fn ensure_template_parenthesized_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
) -> Result<(), ParseError> {
    ensure_template_token(command, pos, token, next, |token| {
        if !token.starts_with('(') {
            return Err(template_parse_error(
                "template token parser returned token without opening parenthesis",
            ));
        }
        if !token.ends_with(')') {
            return Err(template_parse_error(
                "template token parser returned token without closing parenthesis",
            ));
        }
        Ok(())
    })
}
