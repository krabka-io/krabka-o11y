use super::{
    BTreeMap, BTreeSet, ListArray, RecordBatch, StructArray, TagCatalog, TracesError,
    collect_nested_metadata, optional_list_column,
};

pub(crate) fn collect_nested_column_metadata(
    batch: &RecordBatch,
    column: &str,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
    mut collect: impl FnMut(&StructArray, &mut TagCatalog<'_>) -> Result<(), TracesError>,
) -> Result<(), TracesError> {
    let mut catalog = TagCatalog {
        names: tag_names,
        values: tag_values,
    };
    let Some(values) = optional_list_column(batch, column)? else {
        return Ok(());
    };
    collect_nested_metadata(values, |nested| collect(nested, &mut catalog))
}
