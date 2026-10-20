use super::{
    ParseError, TemplateExpression, TemplateRangeBinding, parse_template_variable_name,
    template_parse_error,
};

pub(crate) struct ParsedTemplateRangeExpression {
    pub(crate) binding: TemplateRangeBinding,
    pub(crate) expression: TemplateExpression,
}

struct RangeAssignment<'a> {
    variables: &'a str,
    expression: &'a str,
    is_declaration: bool,
}

pub(crate) fn parse_template_range_expression(
    range_expression: &str,
) -> Result<ParsedTemplateRangeExpression, ParseError> {
    let assignment = range_assignment(range_expression, ":=", true)
        .or_else(|| range_assignment(range_expression, "=", false));
    let Some(assignment) = assignment else {
        return Ok(ParsedTemplateRangeExpression {
            binding: TemplateRangeBinding::Dot,
            expression: TemplateExpression::parse(range_expression.trim())?,
        });
    };
    let variables = assignment
        .variables
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
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
    let binding = if assignment.is_declaration {
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
    Ok(ParsedTemplateRangeExpression {
        binding,
        expression: TemplateExpression::parse(assignment.expression.trim())?,
    })
}

fn range_assignment<'a>(
    input: &'a str,
    delimiter: &str,
    is_declaration: bool,
) -> Option<RangeAssignment<'a>> {
    let delimiter_index = input.find(delimiter)?;
    let variables = &input[..delimiter_index];
    if !variables.split(',').all(|variable| {
        parse_template_variable_name(variable.trim(), "expected template range variable").is_ok()
    }) {
        return None;
    }
    Some(RangeAssignment {
        variables,
        expression: &input[delimiter_index + delimiter.len()..],
        is_declaration,
    })
}
