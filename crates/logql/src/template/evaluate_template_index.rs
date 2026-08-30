use super::{TemplateRuntimeValue, template_collection_first_args, template_index_value};

pub(crate) fn evaluate_template_index(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some((value, indexes)) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    let mut current = value.clone();
    for index in indexes {
        let Some(element) = template_index_value(&current, &index.as_rendered_string()) else {
            return TemplateRuntimeValue::String(String::new());
        };
        current = element;
    }
    current
}
