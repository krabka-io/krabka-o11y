use super::{
    ParseError, ParsedTemplateValue, TemplateConditional, TemplateControlExpression,
    find_template_control_action, parse_template_parts, template_parse_error,
};

pub(crate) fn parse_template_conditional(
    template: &str,
    mut branch_start: usize,
    first_condition: &str,
) -> Result<ParsedTemplateValue<TemplateConditional>, ParseError> {
    let mut branches = Vec::new();
    let mut condition = TemplateControlExpression::parse(first_condition)?;
    loop {
        let Some(control) = find_template_control_action(template, branch_start)? else {
            return Err(template_parse_error("expected template end action"));
        };
        let branch_parts = parse_template_parts(&template[branch_start..control.body_end])?;
        if let Some(next_condition) = control.expression.strip_prefix("else if ") {
            branches.push(super::TemplateConditionalBranch {
                condition,
                parts: branch_parts,
            });
            condition = TemplateControlExpression::parse(next_condition.trim())?;
            branch_start = control.next_position;
            continue;
        }
        if control.expression == "else" {
            branches.push(super::TemplateConditionalBranch {
                condition,
                parts: branch_parts,
            });
            let Some(else_control) = find_template_control_action(template, control.next_position)?
            else {
                return Err(template_parse_error("expected template end action"));
            };
            if else_control.expression != "end" {
                return Err(template_parse_error("unexpected template control action"));
            }
            let else_parts =
                parse_template_parts(&template[control.next_position..else_control.body_end])?;
            return Ok(ParsedTemplateValue {
                value: TemplateConditional {
                    branches,
                    else_parts,
                },
                next_position: else_control.next_position,
            });
        }
        branches.push(super::TemplateConditionalBranch {
            condition,
            parts: branch_parts,
        });
        return Ok(ParsedTemplateValue {
            value: TemplateConditional {
                branches,
                else_parts: Vec::new(),
            },
            next_position: control.next_position,
        });
    }
}
