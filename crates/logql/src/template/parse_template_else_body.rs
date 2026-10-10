use super::{
    ParseError, TemplatePart, find_template_control_action, parse_template_parts,
    template_parse_error,
};

/// Parses an `{{else}}` body that runs to its `{{end}}`, returning the body's
/// parts and the position just past `{{end}}`.
pub(crate) fn parse_template_else_body(
    template: &str,
    else_body_start: usize,
) -> Result<(Vec<TemplatePart>, usize), ParseError> {
    let Some((end_body, end_expression, end_next)) =
        find_template_control_action(template, else_body_start)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    if end_expression != "end" {
        return Err(template_parse_error("unexpected template control action"));
    }
    let else_parts = parse_template_parts(&template[else_body_start..end_body])?;
    Ok((else_parts, end_next))
}
