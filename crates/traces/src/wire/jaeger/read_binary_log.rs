use super::{BT_I64, BT_LIST, BinaryInput, JaegerLog, WireError, read_binary_key_value};

pub(crate) fn read_binary_log(input: &mut BinaryInput<'_>) -> Result<JaegerLog, WireError> {
    let mut out = JaegerLog::default();
    while let Some(field) = input.read_field()? {
        match field.field_id {
            1 if field.field_type == BT_I64 => out.timestamp_micros = input.read_i64()?,
            2 if field.field_type == BT_LIST => {
                out.fields = input.read_struct_list(read_binary_key_value)?;
            }
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
