use super::{TemplatePart, ParseError, template_action_trim_left, parse_template_action, parse_template_conditional, parse_template_range, parse_template_with, is_template_comment_action, parse_template_assignment, is_unexpected_template_control_action, template_parse_error, TemplateExpression};

pub(crate) fn parse_template_parts(template: &str) -> Result<Vec<TemplatePart>, ParseError> {
    let mut parts = Vec::new();
    let mut pos = 0;
    while let Some(rest) = template.get(pos..) {
        if rest.is_empty() {
            break;
        }
        let Some(open_offset) = rest.find("{{") else {
            parts.push(TemplatePart::Literal(rest.to_string()));
            break;
        };
        let open = pos
            .checked_add(open_offset)
            .expect("template action offset cannot overflow");
        let literal = &template[pos..open];
        if !literal.is_empty() {
            let literal = if template_action_trim_left(template, open)? {
                literal.trim_end_matches(char::is_whitespace).to_string()
            } else {
                literal.to_string()
            };
            parts.push(TemplatePart::Literal(literal));
        }

        let action = parse_template_action(template, open)?;
        let expression = action.expression;
        if let Some(condition) = expression.strip_prefix("if ") {
            let (conditional, next_pos) =
                parse_template_conditional(template, action.next_pos, condition.trim())?;
            parts.push(TemplatePart::Conditional(conditional));
            pos = next_pos;
            continue;
        }
        if let Some(range_expression) = expression.strip_prefix("range ") {
            let (range, next_pos) =
                parse_template_range(template, action.next_pos, range_expression)?;
            parts.push(TemplatePart::Range(range));
            pos = next_pos;
            continue;
        }
        if let Some(with_expression) = expression.strip_prefix("with ") {
            let (with, next_pos) = parse_template_with(template, action.next_pos, with_expression)?;
            parts.push(TemplatePart::With(with));
            pos = next_pos;
            continue;
        }
        if is_template_comment_action(expression) {
            parts.push(TemplatePart::Comment);
            pos = action.next_pos;
            continue;
        }
        if let Some(assignment) = parse_template_assignment(expression)? {
            parts.push(TemplatePart::Assignment(assignment));
            pos = action.next_pos;
            continue;
        }
        if is_unexpected_template_control_action(expression) {
            return Err(template_parse_error("unexpected template control action"));
        }
        parts.push(TemplatePart::Expression(TemplateExpression::parse(
            expression,
        )?));
        pos = action.next_pos;
    }
    Ok(parts)
}
