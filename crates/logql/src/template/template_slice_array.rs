use super::{TemplateRuntimeValue, template_slice_bounds};

pub(crate) fn template_slice_array(
    values: &[serde_json::Value],
    bounds: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    let Some((start, end)) = template_slice_bounds(values.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::Json(serde_json::Value::Array(values[start..end].to_vec()))
}
