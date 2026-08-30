use super::{
    ParseError, is_template_control_assignment_variable_char, parse_template_variable_name,
};

pub(crate) fn parse_template_control_assignment(
    expression: &str,
) -> Result<Option<(String, &str)>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let Some((variable, expression)) = expression.split_once(":=") else {
        return Ok(None);
    };
    if variable
        .trim()
        .contains(is_template_control_assignment_variable_char)
    {
        return Ok(None);
    }
    Ok(Some((
        parse_template_variable_name(variable.trim(), "expected template variable")?,
        expression.trim(),
    )))
}
