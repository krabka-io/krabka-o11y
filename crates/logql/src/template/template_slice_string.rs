use super::{TemplateRuntimeValue, template_slice_bounds};

pub(crate) fn template_slice_string(
    value: &str,
    bounds: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    let Some(bounds) = template_slice_bounds(value.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::Bytes(value.as_bytes()[bounds.start..bounds.end].to_vec())
}
