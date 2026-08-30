use super::*;

pub(crate) fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<StructuredMetadata, BlockStoreError> {
    let entries = metadata.value(row);
    let keys = entries
        .column_by_name("key")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "structured_metadata.key",
            expected: "Utf8",
        })?;
    let values = entries
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "structured_metadata.value",
            expected: "Utf8",
        })?;

    Ok((0..entries.len())
        .map(|index| {
            (
                keys.value(index).to_string(),
                values.value(index).to_string(),
            )
        })
        .collect())
}
