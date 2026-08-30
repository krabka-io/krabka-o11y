use super::{
    Array, EventRef, RecordBatch, SCOL_EVENTS, StructArray, Time, TimeExt, TraceqlError,
    nested_string_attrs, optional_list_column, string_array_value, struct_int64_field,
    struct_list_field, struct_string_field,
};

pub(crate) fn event_values(batch: &RecordBatch, row: usize) -> Result<Vec<EventRef>, TraceqlError> {
    let Some(events) = optional_list_column(batch, SCOL_EVENTS)? else {
        return Ok(Vec::new());
    };
    if events.is_null(row) {
        return Ok(Vec::new());
    }
    let row_events = events.value(row);
    let row_events = row_events
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_EVENTS}` row is not a struct"))
        })?;
    let names = struct_string_field(row_events, 0, SCOL_EVENTS)?;
    let times = struct_int64_field(row_events, 1, SCOL_EVENTS)?;
    let attr_keys = struct_list_field(row_events, 2, SCOL_EVENTS)?;
    let attr_values = struct_list_field(row_events, 3, SCOL_EVENTS)?;

    let mut out = Vec::new();
    for idx in 0..row_events.len() {
        if row_events.is_null(idx) {
            continue;
        }
        let name = if names.is_null(idx) {
            String::new()
        } else {
            string_array_value(names, idx)?
        };
        let time_since_start = if times.is_null(idx) {
            <Time as TimeExt>::ZERO
        } else {
            Time::from_nanos(times.value(idx))
        };
        out.push(EventRef {
            time_since_start,
            name,
            attributes: nested_string_attrs(attr_keys, attr_values, idx)?,
        });
    }
    Ok(out)
}
