use super::{CompactInput, JaegerLog, T_I64, T_LIST, WireError, read_key_value};

pub(crate) fn read_log(input: &mut CompactInput<'_>) -> Result<JaegerLog, WireError> {
    let mut out = JaegerLog::default();
    let mut last = 0;
    while let Some(field) = input.read_field(&mut last)? {
        match field.field_id {
            1 if field.field_type == T_I64 => out.timestamp_micros = input.read_i64()?,
            2 if field.field_type == T_LIST => {
                out.fields = input.read_struct_list(read_key_value)?;
            }
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
