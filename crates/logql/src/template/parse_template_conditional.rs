use super::*;

pub(crate) fn parse_template_conditional(
    template: &str,
    mut branch_start: usize,
    first_condition: &str,
) -> Result<(TemplateConditional, usize), ParseError> {
    let mut branches = Vec::new();
    let mut condition = TemplateControlExpression::parse(first_condition)?;
    loop {
        let Some((body_end, expression, next_pos)) =
            find_template_control_action(template, branch_start)?
        else {
            return Err(template_parse_error("expected template end action"));
        };
        let branch_parts = parse_template_parts(&template[branch_start..body_end])?;
        if let Some(next_condition) = expression.strip_prefix("else if ") {
            branches.push((condition, branch_parts));
            condition = TemplateControlExpression::parse(next_condition.trim())?;
            branch_start = next_pos;
            continue;
        }
        if expression == "else" {
            branches.push((condition, branch_parts));
            let Some((else_end_body, end_expression, else_end_next)) =
                find_template_control_action(template, next_pos)?
            else {
                return Err(template_parse_error("expected template end action"));
            };
            if end_expression != "end" {
                return Err(template_parse_error("unexpected template control action"));
            }
            let else_parts = parse_template_parts(&template[next_pos..else_end_body])?;
            return Ok((
                TemplateConditional {
                    branches,
                    else_parts,
                },
                else_end_next,
            ));
        }
        branches.push((condition, branch_parts));
        return Ok((
            TemplateConditional {
                branches,
                else_parts: Vec::new(),
            },
            next_pos,
        ));
    }
}
