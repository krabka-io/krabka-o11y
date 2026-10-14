use super::{
    ParseError, TemplateAssignment, TemplateExpression,
    is_template_control_assignment_variable_char, parse_template_variable_name,
};

pub(crate) fn parse_template_assignment(
    expression: &str,
) -> Result<Option<TemplateAssignment>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let expression_declaration = expression.contains(":=");
    let parts = if let Some(parts) = expression.split_once(":=") {
        parts
    } else if let Some(parts) = expression.split_once('=') {
        if parts
            .0
            .trim()
            .contains(is_template_control_assignment_variable_char)
        {
            return Ok(None);
        }
        parts
    } else {
        return Ok(None);
    };
    let variable = parts.0;
    let expression = parts.1;
    let variable = parse_template_variable_name(variable.trim(), "expected template variable")?;
    Ok(Some(TemplateAssignment {
        variable,
        declare: expression_declaration,
        expression: TemplateExpression::parse(expression.trim())?,
    }))
}
