use super::{TemplateRuntimeValue, template_slice_bounds};

pub(crate) fn template_slice_string(
    value: &str,
    bounds: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    let Some((start, end)) = template_slice_bounds(value.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::Bytes(value.as_bytes()[start..end].to_vec())
}
