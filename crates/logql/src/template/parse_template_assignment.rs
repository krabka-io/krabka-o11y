use super::*;

pub(crate) fn parse_template_assignment(expression: &str) -> Result<Option<TemplateAssignment>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let (variable, expression) = if let Some((variable, expression)) = expression.split_once(":=") {
        (variable, expression)
    } else if let Some((variable, expression)) = expression.split_once('=') {
        if variable
            .trim()
            .contains(is_template_control_assignment_variable_char)
        {
            return Ok(None);
        }
        (variable, expression)
    } else {
        return Ok(None);
    };
    let variable = parse_template_variable_name(variable.trim(), "expected template variable")?;
    Ok(Some(TemplateAssignment {
        variable,
        expression: TemplateExpression::parse(expression.trim())?,
    }))
}
