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
            attributes: nested_typed_attrs(row_events, idx)?.unwrap_or(nested_string_attrs(
                attr_keys,
                attr_values,
                idx,
            )?),
        });
    }
    Ok(out)
}

pub(super) fn nested_typed_attrs(
    structs: &StructArray,
    row: usize,
) -> Result<Option<Vec<(String, super::AttrValue)>>, TraceqlError> {
    let Some(payload) = structs.column_by_name("attr_typed") else {
        return Ok(None);
    };
    if payload.is_null(row) {
        return Ok(None);
    }
    let payload = string_array_value(payload.as_ref(), row)?;
    let attrs: Vec<krabka_blockstore::SpanAttr> =
        serde_json::from_str(&payload).map_err(|error| {
            TraceqlError::Store(format!("invalid typed nested attributes: {error}"))
        })?;
    Ok(Some(
        attrs
            .into_iter()
            .flat_map(|attr| {
                use krabka_blockstore::AttrValue as BlockValue;

                use super::AttrValue;
                let values = match attr.value {
                    BlockValue::Unsupported(value) => {
                        return vec![(attr.key, super::AttrValue::Unsupported(value))];
                    }
                    BlockValue::Str(values) => {
                        values.into_iter().map(AttrValue::Str).collect::<Vec<_>>()
                    }
                    BlockValue::Int(values) => values.into_iter().map(AttrValue::Int).collect(),
                    BlockValue::Double(values) => {
                        values.into_iter().map(AttrValue::Float).collect()
                    }
                    BlockValue::Bool(values) => values.into_iter().map(AttrValue::Bool).collect(),
                };
                if attr.is_array {
                    vec![(attr.key, AttrValue::Array(values))]
                } else {
                    values
                        .into_iter()
                        .map(|value| (attr.key.clone(), value))
                        .collect()
                }
            })
            .collect(),
    ))
}
