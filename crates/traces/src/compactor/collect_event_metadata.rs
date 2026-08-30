use super::{RecordBatch, BTreeSet, BTreeMap, TracesError, optional_list_column, SCOL_EVENTS, collect_nested_metadata, struct_string_field, struct_i64_field, struct_list_field, Array, insert_tag_value, collect_nested_attrs};

pub(crate) fn collect_event_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let Some(events) = optional_list_column(batch, SCOL_EVENTS)? else {
        return Ok(());
    };
    collect_nested_metadata(events, |event| {
        let names = struct_string_field(event, 0)?;
        let times = struct_i64_field(event, 1)?;
        let keys = struct_list_field(event, 2)?;
        let values = struct_list_field(event, 3)?;
        for idx in 0..event.len() {
            if event.is_null(idx) {
                continue;
            }
            if !names.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "event:name",
                    names.value(idx).to_string(),
                );
            }
            if !times.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "event:timeSinceStart",
                    times.value(idx).to_string(),
                );
            }
            collect_nested_attrs(keys, values, idx, tag_names, tag_values)?;
        }
        Ok(())
    })
}
