use super::{BinaryInput, JaegerLog, WireError, BT_I64, BT_LIST, read_binary_key_value};

pub(crate) fn read_binary_log(input: &mut BinaryInput<'_>) -> Result<JaegerLog, WireError> {
    let mut out = JaegerLog::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_I64) => out.timestamp_micros = input.read_i64()?,
            (2, BT_LIST) => out.fields = input.read_struct_list(read_binary_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
