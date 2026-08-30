use super::*;

pub(crate) fn parse_string_arg(input: &str) -> Result<String, ParseError> {
    let mut p = Parser::new(input);
    let value = p.parse_quoted()?;
    p.skip_ws();
    if p.pos == input.len() {
        Ok(value)
    } else {
        Err(syntax_error("expected string argument"))
    }
}
