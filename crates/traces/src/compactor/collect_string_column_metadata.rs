use super::{Array, BTreeMap, BTreeSet, RecordBatch, StringArray, TagCatalog, TracesError};

pub(crate) fn collect_string_column_metadata(
    batch: &RecordBatch,
    column: &str,
    tag: &str,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let mut catalog = TagCatalog {
        names: tag_names,
        values: tag_values,
    };
    let Some(col) = batch.column_by_name(column) else {
        return Ok(());
    };
    let strings = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} is not Utf8")))?;
    for row in 0..strings.len() {
        if strings.is_null(row) || strings.value(row).is_empty() {
            continue;
        }
        catalog.insert(tag, strings.value(row).to_string());
    }
    Ok(())
}
