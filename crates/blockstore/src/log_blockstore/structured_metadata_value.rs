use super::*;

pub(crate) fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<StructuredMetadata, BlockStoreError> {
    let entries = crate::map_array_string_entries(metadata, row, |column, expected| {
        BlockStoreError::InvalidBlockColumn { column, expected }
    })?;
    let mut result = StructuredMetadata::new();
    for entry in entries {
        result.insert(entry.key, entry.value);
    }
    Ok(result)
}
