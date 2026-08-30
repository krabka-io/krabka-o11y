use super::{TemplateRange, ParseError, parse_template_range_expression, find_template_control_action, template_parse_error, parse_template_parts};

pub(crate) fn parse_template_range(
    template: &str,
    body_start: usize,
    range_expression: &str,
) -> Result<(TemplateRange, usize), ParseError> {
    let (binding, expression) = parse_template_range_expression(range_expression)?;
    let Some((control_body, control_expression, control_next)) =
        find_template_control_action(template, body_start)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    let parts = parse_template_parts(&template[body_start..control_body])?;
    if control_expression == "end" {
        return Ok((
            TemplateRange {
                binding,
                expression,
                parts,
                else_parts: Vec::new(),
            },
            control_next,
        ));
    }
    if control_expression != "else" {
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
        TemplateRange {
            binding,
            expression,
            parts,
            else_parts,
        },
        end_next,
    ))
}
