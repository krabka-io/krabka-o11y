use arrow::array::{Array, MapArray, StringArray};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StringMapEntry {
    pub key: String,
    pub value: String,
}

/// Reads a row from an Arrow map whose keys and values are UTF-8 strings.
///
/// `invalid_column` receives the missing or incorrectly typed column name and
/// its expected Arrow type, allowing callers to preserve their own error type.
///
/// # Errors
/// Returns the error produced by `invalid_column` when either map column is
/// absent or is not UTF-8.
pub fn map_array_string_entries<E>(
    map: &MapArray,
    row: usize,
    invalid_column: impl Fn(&'static str, &'static str) -> E,
) -> Result<Vec<StringMapEntry>, E> {
    let entries = map.value(row);
    let keys = entries
        .column_by_name("key")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid_column("structured_metadata.key", "Utf8"))?;
    let values = entries
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid_column("structured_metadata.value", "Utf8"))?;

    Ok((0..entries.len())
        .map(|index| {
            StringMapEntry {
                key: keys.value(index).to_string(),
                value: values.value(index).to_string(),
            }
        })
        .collect())
}
