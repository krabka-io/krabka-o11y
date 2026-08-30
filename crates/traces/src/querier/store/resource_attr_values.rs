use super::*;

pub(crate) fn resource_attr_values(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    Ok(attr_values_with_resource(batch, row, true)?
        .into_iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(RESOURCE_ATTR_PREFIX)
                .map(|key| (key.to_string(), value))
        })
        .collect())
}
