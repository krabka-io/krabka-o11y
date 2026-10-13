use super::{
    Array, BTreeMap, BTreeSet, RecordBatch, SCOL_LINKS, TracesError, collect_nested_attrs,
    collect_nested_column_metadata, struct_fixed_field, struct_list_field,
};

pub(crate) fn collect_link_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    collect_nested_column_metadata(batch, SCOL_LINKS, tag_names, tag_values, |link, catalog| {
        let trace_ids = struct_fixed_field(link, 0)?;
        let span_ids = struct_fixed_field(link, 1)?;
        let keys = struct_list_field(link, 2)?;
        let values = struct_list_field(link, 3)?;
        for idx in 0..link.len() {
            if link.is_null(idx) {
                continue;
            }
            if !trace_ids.is_null(idx) {
                catalog.insert("link:traceID", hex::encode(trace_ids.value(idx)));
            }
            if !span_ids.is_null(idx) {
                catalog.insert("link:spanID", hex::encode(span_ids.value(idx)));
            }
            collect_nested_attrs(keys, values, idx, catalog.names, catalog.values)?;
        }
        Ok(())
    })
}
