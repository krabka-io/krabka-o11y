use super::*;

pub(crate) fn template_action_trim_left(template: &str, open: usize) -> Result<bool, ParseError> {
    let expression_start = open + 2;
    if !template[expression_start..].starts_with('-') {
        return Ok(false);
    }
    let Some(next) = template[expression_start + 1..].chars().next() else {
        return Err(template_parse_error("expected closing template action"));
    };
    Ok(next.is_whitespace())
}
