use super::{
    ParseError, ParsedTemplateValue, TemplateRange, parse_template_control_body,
    parse_template_else_body, parse_template_range_expression, template_parse_error,
};

pub(crate) fn parse_template_range(
    template: &str,
    body_start: usize,
    range_expression: &str,
) -> Result<ParsedTemplateValue<TemplateRange>, ParseError> {
    let parsed_expression = parse_template_range_expression(range_expression)?;
    let control = parse_template_control_body(template, body_start)?;
    if control.expression == "end" {
        return Ok(ParsedTemplateValue {
            value: TemplateRange {
                binding: parsed_expression.binding,
                expression: parsed_expression.expression,
                parts: control.parts,
                else_parts: Vec::new(),
            },
            next_position: control.next_position,
        });
    }
    if control.expression != "else" {
        return Err(template_parse_error("unexpected template control action"));
    }

    let parsed_else = parse_template_else_body(template, control.next_position)?;
    Ok(ParsedTemplateValue {
        value: TemplateRange {
            binding: parsed_expression.binding,
            expression: parsed_expression.expression,
            parts: control.parts,
            else_parts: parsed_else.value,
        },
        next_position: parsed_else.next_position,
    })
}
