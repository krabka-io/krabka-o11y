use super::{
    Array, BTreeMap, BTreeSet, RecordBatch, SCOL_EVENTS, TracesError, collect_nested_attrs,
    collect_nested_column_metadata, struct_i64_field, struct_list_field, struct_string_field,
};

pub(crate) fn collect_event_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    collect_nested_column_metadata(
        batch,
        SCOL_EVENTS,
        tag_names,
        tag_values,
        |event, catalog| {
            let names = struct_string_field(event, 0)?;
            let times = struct_i64_field(event, 1)?;
            let keys = struct_list_field(event, 2)?;
            let values = struct_list_field(event, 3)?;
            for idx in 0..event.len() {
                if event.is_null(idx) {
                    continue;
                }
                if !names.is_null(idx) {
                    catalog.insert("event:name", names.value(idx).to_string());
                }
                if !times.is_null(idx) {
                    catalog.insert("event:timeSinceStart", times.value(idx).to_string());
                }
                collect_nested_attrs(keys, values, idx, catalog.names, catalog.values)?;
            }
            Ok(())
        },
    )
}
