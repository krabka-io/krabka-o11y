use super::{ParseError, template_parse_error};

pub(crate) fn ensure_template_parenthesized_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
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
}
