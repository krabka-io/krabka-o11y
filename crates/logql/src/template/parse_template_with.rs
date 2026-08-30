use super::*;

pub(crate) fn parse_template_with(
    template: &str,
    body_start: usize,
    with_expression: &str,
) -> Result<(TemplateWith, usize), ParseError> {
    let expression = TemplateControlExpression::parse(with_expression.trim())?;
    let Some((control_body, control_expression, control_next)) =
        find_template_control_action(template, body_start)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    let parts = parse_template_parts(&template[body_start..control_body])?;
    if control_expression == "end" {
        return Ok((
            TemplateWith {
                expression,
                parts,
                else_parts: Vec::new(),
            },
            control_next,
        ));
    }
    if control_expression != "else" {
        if let Some(with_expression) = control_expression.strip_prefix("else with ") {
            let (with, next_pos) = parse_template_with(template, control_next, with_expression)?;
            return Ok((
                TemplateWith {
                    expression,
                    parts,
                    else_parts: vec![TemplatePart::With(with)],
                },
                next_pos,
            ));
        }
        return Err(template_parse_error("unexpected template control action"));
    }

    let Some((end_body, end_expression, end_next)) =
        find_template_control_action(template, control_next)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    if end_expression != "end" {
        return Err(template_parse_error("unexpected template control action"));
    }
    let else_parts = parse_template_parts(&template[control_next..end_body])?;
    Ok((
        TemplateWith {
            expression,
            parts,
            else_parts,
        },
        end_next,
    ))
}
