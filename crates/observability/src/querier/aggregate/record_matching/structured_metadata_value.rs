use super::{Labels, MapArray, QueryError};

pub(crate) fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<Labels, QueryError> {
    let entries =
        krabka_blockstore::map_array_string_entries(metadata, row, |column, expected| {
            QueryError::InvalidColumn { column, expected }
        })?;
    let mut labels = Labels::new();
    for entry in entries {
        labels.insert(entry.key, entry.value);
    }
    Ok(labels)
}
