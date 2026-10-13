use super::{
    ParseError, ParsedTemplateValue, TemplatePart, find_template_control_action,
    parse_template_parts, template_parse_error,
};

/// Parses an `{{else}}` body that runs to its `{{end}}`, returning the body's
/// parts and the position just past `{{end}}`.
pub(crate) fn parse_template_else_body(
    template: &str,
    else_body_start: usize,
) -> Result<ParsedTemplateValue<Vec<TemplatePart>>, ParseError> {
    let Some(control) = find_template_control_action(template, else_body_start)? else {
        return Err(template_parse_error("expected template end action"));
    };
    if control.expression != "end" {
        return Err(template_parse_error("unexpected template control action"));
    }
    let else_parts = parse_template_parts(&template[else_body_start..control.body_end])?;
    Ok(ParsedTemplateValue {
        value: else_parts,
        next_position: control.next_position,
    })
}
