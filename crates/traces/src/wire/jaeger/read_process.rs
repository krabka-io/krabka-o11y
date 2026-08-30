use super::{CompactInput, JaegerProcess, T_BINARY, T_LIST, WireError, read_key_value};

pub(crate) fn read_process(input: &mut CompactInput<'_>) -> Result<JaegerProcess, WireError> {
    let mut out = JaegerProcess::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_BINARY) => out.service_name = input.read_string()?,
            (2, T_LIST) => out.tags = input.read_struct_list(read_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
