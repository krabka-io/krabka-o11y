use super::*;

pub(crate) fn evaluate_template_slice(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some((value, bounds)) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    match value {
        TemplateRuntimeValue::String(value) => template_slice_string(value, bounds),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => {
            template_slice_string(value, bounds)
        }
        TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
            template_slice_array(values, bounds)
        }
        _ => TemplateRuntimeValue::String(String::new()),
    }
}
