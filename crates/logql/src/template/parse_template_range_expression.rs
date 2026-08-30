use super::*;

pub(crate) fn parse_template_range_expression(
    range_expression: &str,
) -> Result<(TemplateRangeBinding, TemplateExpression), ParseError> {
    let Some((variables, expression)) = range_expression.split_once(":=") else {
        return Ok((
            TemplateRangeBinding::Dot,
            TemplateExpression::parse(range_expression.trim())?,
        ));
    };
    let variables = variables.split(',').map(str::trim).collect::<Vec<_>>();
    let binding = match variables.as_slice() {
        [variable] => TemplateRangeBinding::Value(parse_template_variable_name(
            variable,
            "expected template range variable",
        )?),
        [index, value] => TemplateRangeBinding::IndexValue {
            index: parse_template_variable_name(index, "expected template range variable")?,
            value: parse_template_variable_name(value, "expected template range variable")?,
        },
        _ => return Err(template_parse_error("expected template range variable")),
    };
    Ok((binding, TemplateExpression::parse(expression.trim())?))
}
