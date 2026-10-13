use super::{
    CompactInput, JaegerProcess, KeyValueReader, T_BINARY, T_LIST, WireError, read_key_value,
};

pub(crate) fn read_process(input: &mut CompactInput<'_>) -> Result<JaegerProcess, WireError> {
    let mut out = JaegerProcess::default();
    let mut last = 0;
    while let Some(field) = input.read_field(&mut last)? {
        match field.field_id {
            1 if field.field_type == T_BINARY => out.service_name = input.read_string()?,
            2 if field.field_type == T_LIST => out.tags = input.read_struct_list(read_key_value)?,
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
