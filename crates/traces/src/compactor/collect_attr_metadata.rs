use super::{RecordBatch, BTreeSet, BTreeMap, TracesError, list_column, SCOL_ATTR_KEYS, Array, StringArray, RESOURCE_ATTR_PREFIX, attr_value, insert_tag_value};

pub(crate) fn collect_attr_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let keys = list_column(batch, SCOL_ATTR_KEYS)?;
    for row in 0..batch.num_rows() {
        if keys.is_null(row) {
            continue;
        }
        let row_keys = keys.value(row);
        let row_keys = row_keys
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TracesError::Block("attr_keys row is not Utf8".into()))?;
        for idx in 0..row_keys.len() {
            if row_keys.is_null(idx) {
                continue;
            }
            let key = row_keys
                .value(idx)
                .strip_prefix(RESOURCE_ATTR_PREFIX)
                .unwrap_or_else(|| row_keys.value(idx));
            if let Some(value) = attr_value(batch, row, idx)? {
                insert_tag_value(tag_names, tag_values, key, value);
            } else {
                tag_names.insert(key.to_string());
            }
        }
    }
    Ok(())
}
