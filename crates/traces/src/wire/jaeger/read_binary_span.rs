use super::*;

pub(crate) fn read_binary_span(input: &mut BinaryInput<'_>) -> Result<JaegerSpan, WireError> {
    let mut out = JaegerSpan::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_I64) => out.trace_id_low = input.read_i64()?,
            (2, BT_I64) => out.trace_id_high = input.read_i64()?,
            (3, BT_I64) => out.span_id = input.read_i64()?,
            (4, BT_I64) => out.parent_span_id = input.read_i64()?,
            (5, BT_BINARY) => out.operation_name = input.read_string()?,
            (6, BT_LIST) => out.references = input.read_struct_list(read_binary_ref)?,
            (8, BT_I64) => out.start_time_micros = input.read_i64()?,
            (9, BT_I64) => out.duration_micros = input.read_i64()?,
            (10, BT_LIST) => out.tags = input.read_struct_list(read_binary_key_value)?,
            (11, BT_LIST) => out.logs = input.read_struct_list(read_binary_log)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
