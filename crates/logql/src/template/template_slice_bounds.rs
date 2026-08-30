use super::{TemplateRuntimeValue, parse_template_bound};

pub(crate) fn template_slice_bounds(len: usize, bounds: &[TemplateRuntimeValue]) -> Option<(usize, usize)> {
    if bounds.len() > 3 {
        return None;
    }
    let start = bounds.first().map_or(Some(0), parse_template_bound)?;
    let end = bounds.get(1).map_or(Some(len), parse_template_bound)?;
    if let Some(capacity) = bounds.get(2) {
        let capacity = parse_template_bound(capacity)?;
        if end > capacity || capacity > len {
            return None;
        }
    }
    (start <= end && end <= len).then_some((start, end))
}
