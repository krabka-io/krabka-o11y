use super::*;

pub(crate) fn read_log(input: &mut CompactInput<'_>) -> Result<JaegerLog, WireError> {
    let mut out = JaegerLog::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_I64) => out.timestamp_micros = input.read_i64()?,
            (2, T_LIST) => out.fields = input.read_struct_list(read_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
