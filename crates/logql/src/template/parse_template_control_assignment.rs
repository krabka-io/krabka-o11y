use super::{
    ParseError, is_template_control_assignment_variable_char, parse_template_variable_name,
};

pub(crate) struct TemplateControlAssignment<'a> {
    pub(crate) variable: String,
    pub(crate) expression: &'a str,
}

pub(crate) fn parse_template_control_assignment(
    expression: &str,
) -> Result<Option<TemplateControlAssignment<'_>>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let Some(parts) = expression.split_once(":=") else {
        return Ok(None);
    };
    let variable = parts.0;
    let expression = parts.1;
    if variable
        .trim()
        .contains(is_template_control_assignment_variable_char)
    {
        return Ok(None);
    }
    Ok(Some(TemplateControlAssignment {
        variable: parse_template_variable_name(variable.trim(), "expected template variable")?,
        expression: expression.trim(),
    }))
}
