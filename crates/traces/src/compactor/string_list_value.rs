use super::{Array, ListArray, StringArray};

pub(crate) fn string_list_value(values: &ListArray, idx: usize) -> Option<String> {
    if idx >= values.len() || values.is_null(idx) {
        return None;
    }
    let values = values.value(idx);
    let values = values.as_any().downcast_ref::<StringArray>()?;
    (0..values.len())
        .find(|value_idx| !values.is_null(*value_idx))
        .map(|value_idx| values.value(value_idx).to_string())
}
