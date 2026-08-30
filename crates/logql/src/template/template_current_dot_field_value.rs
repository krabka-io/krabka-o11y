use super::{TemplateRuntimeValue, template_variable_path_value};

pub(crate) fn template_current_dot_field_value(
    value: &TemplateRuntimeValue,
    field: &str,
) -> Option<TemplateRuntimeValue> {
    let path = field
        .split('.')
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    template_variable_path_value(value, &path)
}
