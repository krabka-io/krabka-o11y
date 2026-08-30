use super::*;

pub(crate) fn parse_scalar_text(input: &str) -> bool {
    let mut p = Parser::new(input);
    p.parse_scalar_literal_text().is_ok() && {
        p.skip_ws();
        p.pos == input.len()
    }
}
