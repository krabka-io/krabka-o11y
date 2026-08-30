use super::{RecordBatch, BTreeSet, BTreeMap, TracesError, optional_list_column, SCOL_LINKS, collect_nested_metadata, struct_fixed_field, struct_list_field, Array, insert_tag_value, collect_nested_attrs};

pub(crate) fn collect_link_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let Some(links) = optional_list_column(batch, SCOL_LINKS)? else {
        return Ok(());
    };
    collect_nested_metadata(links, |link| {
        let trace_ids = struct_fixed_field(link, 0)?;
        let span_ids = struct_fixed_field(link, 1)?;
        let keys = struct_list_field(link, 2)?;
        let values = struct_list_field(link, 3)?;
        for idx in 0..link.len() {
            if link.is_null(idx) {
                continue;
            }
            if !trace_ids.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "link:traceID",
                    hex::encode(trace_ids.value(idx)),
                );
            }
            if !span_ids.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "link:spanID",
                    hex::encode(span_ids.value(idx)),
                );
            }
            collect_nested_attrs(keys, values, idx, tag_names, tag_values)?;
        }
        Ok(())
    })
}
