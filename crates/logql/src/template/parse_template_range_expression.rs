use super::{
    ParseError, TemplateExpression, TemplateRangeBinding, parse_template_variable_name,
    template_parse_error,
};

pub(crate) fn parse_template_range_expression(
    range_expression: &str,
) -> Result<(TemplateRangeBinding, TemplateExpression), ParseError> {
    let binding = |(variables, _): &(&str, &str)| {
        variables.split(',').all(|variable| {
            parse_template_variable_name(variable.trim(), "expected template range variable")
                .is_ok()
        })
    };
    let declaration = range_expression.split_once(":=").filter(binding);
    let assignment = range_expression.split_once('=').filter(binding);
    let Some((variables, expression)) = declaration.or(assignment) else {
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
    let binding = if declaration.is_some() {
        binding
    } else {
        match binding {
            TemplateRangeBinding::Value(value) => TemplateRangeBinding::AssignValue(value),
            TemplateRangeBinding::IndexValue { index, value } => {
                TemplateRangeBinding::AssignIndexValue { index, value }
            }
            binding => binding,
        }
    };
    Ok((binding, TemplateExpression::parse(expression.trim())?))
}
