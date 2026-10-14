use super::{
    BT_BINARY, BT_I64, BT_LIST, BinaryInput, JaegerSpan, KeyValueReader, WireError,
    read_binary_key_value, read_binary_log, read_binary_ref,
};

pub(crate) fn read_binary_span(input: &mut BinaryInput<'_>) -> Result<JaegerSpan, WireError> {
    let mut out = JaegerSpan::default();
    while let Some(field) = input.read_field()? {
        match field.field_id {
            1 if field.field_type == BT_I64 => out.trace_id_low = input.read_i64()?,
            2 if field.field_type == BT_I64 => out.trace_id_high = input.read_i64()?,
            3 if field.field_type == BT_I64 => out.span_id = input.read_i64()?,
            4 if field.field_type == BT_I64 => out.parent_span_id = input.read_i64()?,
            5 if field.field_type == BT_BINARY => out.operation_name = input.read_string()?,
            6 if field.field_type == BT_LIST => {
                out.references = input.read_struct_list(read_binary_ref)?;
            }
            8 if field.field_type == BT_I64 => out.start_time_micros = input.read_i64()?,
            9 if field.field_type == BT_I64 => out.duration_micros = input.read_i64()?,
            10 if field.field_type == BT_LIST => {
                out.tags = input.read_struct_list(read_binary_key_value)?;
            }
            11 if field.field_type == BT_LIST => {
                out.logs = input.read_struct_list(read_binary_log)?;
            }
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
