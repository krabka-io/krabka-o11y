use super::*;

pub(crate) fn collect_nested_attrs(
    keys: &ListArray,
    values: &ListArray,
    idx: usize,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    if keys.is_null(idx) {
        return Ok(());
    }
    let attr_keys = keys.value(idx);
    let attr_keys = attr_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block("nested attr keys are not Utf8".into()))?;
    let attr_values = if values.is_null(idx) {
        None
    } else {
        Some(values.value(idx))
    };
    let attr_values = attr_values
        .as_ref()
        .and_then(|array| array.as_any().downcast_ref::<ListArray>());

    for attr_idx in 0..attr_keys.len() {
        if attr_keys.is_null(attr_idx) {
            continue;
        }
        let key = attr_keys.value(attr_idx);
        if let Some(value) = attr_values.and_then(|values| string_list_value(values, attr_idx)) {
            insert_tag_value(tag_names, tag_values, key, value);
        } else {
            tag_names.insert(key.to_string());
        }
    }
    Ok(())
}
