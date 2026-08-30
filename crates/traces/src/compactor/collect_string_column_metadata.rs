use super::*;

pub(crate) fn collect_string_column_metadata(
    batch: &RecordBatch,
    column: &str,
    tag: &str,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
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
        insert_tag_value(tag_names, tag_values, tag, strings.value(row).to_string());
    }
    Ok(())
}
