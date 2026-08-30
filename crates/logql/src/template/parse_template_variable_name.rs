use super::*;

pub(crate) fn parse_template_variable_name(
    variable: &str,
    message: &'static str,
) -> Result<String, ParseError> {
    let Some(variable) = variable.strip_prefix('$') else {
        return Err(template_parse_error(message));
    };
    if variable.is_empty() {
        return Err(template_parse_error(message));
    }
    if variable.contains(is_template_variable_name_char_invalid) {
        return Err(template_parse_error(message));
    }
    Ok(variable.to_string())
}
