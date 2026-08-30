use super::{Array, BTreeMap, BTreeSet, RecordBatch, SCOL_EVENTS, TracesError, collect_nested_attrs, collect_nested_metadata, insert_tag_value, optional_list_column, struct_i64_field, struct_list_field, struct_string_field};

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
