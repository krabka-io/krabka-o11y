use super::{RecordBatch, LinkRef, TraceqlError, optional_list_column, SCOL_LINKS, Array, StructArray, struct_fixed_field, struct_list_field, fixed_array_value, nested_string_attrs};

pub(crate) fn link_values(batch: &RecordBatch, row: usize) -> Result<Vec<LinkRef>, TraceqlError> {
    let Some(links) = optional_list_column(batch, SCOL_LINKS)? else {
        return Ok(Vec::new());
    };
    if links.is_null(row) {
        return Ok(Vec::new());
    }
    let row_links = links.value(row);
    let row_links = row_links
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_LINKS}` row is not a struct"))
        })?;
    let trace_ids = struct_fixed_field(row_links, 0, SCOL_LINKS)?;
    let span_ids = struct_fixed_field(row_links, 1, SCOL_LINKS)?;
    let attr_keys = struct_list_field(row_links, 2, SCOL_LINKS)?;
    let attr_values = struct_list_field(row_links, 3, SCOL_LINKS)?;

    let mut out = Vec::new();
    for idx in 0..row_links.len() {
        if row_links.is_null(idx) {
            continue;
        }
        out.push(LinkRef {
            trace_id: fixed_array_value::<16>(trace_ids, idx, SCOL_LINKS)?,
            span_id: fixed_array_value::<8>(span_ids, idx, SCOL_LINKS)?,
            attributes: nested_string_attrs(attr_keys, attr_values, idx)?,
        });
    }
    Ok(out)
}
