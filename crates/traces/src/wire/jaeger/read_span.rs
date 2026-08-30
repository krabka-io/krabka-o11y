use super::{CompactInput, JaegerSpan, WireError, T_I64, T_BINARY, T_LIST, read_ref, read_key_value, read_log};

pub(crate) fn read_span(input: &mut CompactInput<'_>) -> Result<JaegerSpan, WireError> {
    let mut out = JaegerSpan::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_I64) => out.trace_id_low = input.read_i64()?,
            (2, T_I64) => out.trace_id_high = input.read_i64()?,
            (3, T_I64) => out.span_id = input.read_i64()?,
            (4, T_I64) => out.parent_span_id = input.read_i64()?,
            (5, T_BINARY) => out.operation_name = input.read_string()?,
            (6, T_LIST) => out.references = input.read_struct_list(read_ref)?,
            (8, T_I64) => out.start_time_micros = input.read_i64()?,
            (9, T_I64) => out.duration_micros = input.read_i64()?,
            (10, T_LIST) => out.tags = input.read_struct_list(read_key_value)?,
            (11, T_LIST) => out.logs = input.read_struct_list(read_log)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
