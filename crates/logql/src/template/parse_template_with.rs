use super::{
    ParseError, ParsedTemplateValue, TemplateControlExpression, TemplatePart, TemplateWith,
    parse_template_control_body, parse_template_else_body, template_parse_error,
};

pub(crate) fn parse_template_with(
    template: &str,
    body_start: usize,
    with_expression: &str,
) -> Result<ParsedTemplateValue<TemplateWith>, ParseError> {
    let expression = TemplateControlExpression::parse(with_expression.trim())?;
    let control = parse_template_control_body(template, body_start)?;
    if control.expression == "end" {
        return Ok(ParsedTemplateValue {
            value: TemplateWith {
                expression,
                parts: control.parts,
                else_parts: Vec::new(),
            },
            next_position: control.next_position,
        });
    }
    if control.expression != "else" {
        if let Some(with_expression) = control.expression.strip_prefix("else with ") {
            let parsed_with =
                parse_template_with(template, control.next_position, with_expression)?;
            return Ok(ParsedTemplateValue {
                value: TemplateWith {
                    expression,
                    parts: control.parts,
                    else_parts: vec![TemplatePart::With(parsed_with.value)],
                },
                next_position: parsed_with.next_position,
            });
        }
        return Err(template_parse_error("unexpected template control action"));
    }

    let parsed_else = parse_template_else_body(template, control.next_position)?;
    Ok(ParsedTemplateValue {
        value: TemplateWith {
            expression,
            parts: control.parts,
            else_parts: parsed_else.value,
        },
        next_position: parsed_else.next_position,
    })
}
