use super::{
    Attribute, RESOURCE_ATTR_PREFIX, RecordBatch, ResourceAttrs, TraceqlError,
    attr_values_with_resource,
};

pub(crate) fn resource_attr_values(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<Attribute>, TraceqlError> {
    Ok(
        attr_values_with_resource(batch, row, ResourceAttrs::Include)?
            .into_iter()
            .filter_map(|attribute| {
                attribute
                    .key
                    .strip_prefix(RESOURCE_ATTR_PREFIX)
                    .map(|key| Attribute::new(key, attribute.value))
            })
            .collect(),
    )
}
