use super::{
    ParseError, TemplateExpression, TemplatePart, is_template_comment_action,
    is_unexpected_template_control_action, parse_template_action, parse_template_assignment,
    parse_template_conditional, parse_template_range, parse_template_with,
    template_action_trim_left, template_parse_error,
};

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
        if let Some(named) = expression
            .strip_prefix("define ")
            .or_else(|| expression.strip_prefix("block "))
            .or_else(|| expression.strip_prefix("template "))
        {
            let tokens = super::tokenize_template_command(named)?;
            let name = tokens
                .first()
                .and_then(|token| super::quoted_template_token_value(token).ok().flatten())
                .ok_or_else(|| template_parse_error("expected quoted template name"))?;
            let argument = if tokens.len() > 1 {
                Some(TemplateExpression::parse(&tokens[1..].join(" "))?)
            } else {
                None
            };
            if expression.starts_with("template ") {
                parts.push(TemplatePart::Invocation { name, argument });
                pos = action.next_pos;
                continue;
            }
            if expression.starts_with("define ") && argument.is_some() {
                return Err(template_parse_error("unexpected definition argument"));
            }
            if expression.starts_with("block ") && argument.is_none() {
                return Err(template_parse_error("expected block argument"));
            }
            let (end, control, next_pos) =
                super::find_template_control_action(template, action.next_pos)?
                    .ok_or_else(|| template_parse_error("expected template end action"))?;
            if control != "end" {
                return Err(template_parse_error("unexpected definition control action"));
            }
            let body = parse_template_parts(&template[action.next_pos..end])?;
            parts.push(TemplatePart::Definition {
                name: name.clone(),
                parts: body,
                block: expression.starts_with("block "),
            });
            if expression.starts_with("block ") {
                parts.push(TemplatePart::Invocation { name, argument });
            }
            pos = next_pos;
            continue;
        }
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
        if matches!(expression, "break" | "continue") {
            parts.push(if expression == "break" {
                TemplatePart::Break
            } else {
                TemplatePart::Continue
            });
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
