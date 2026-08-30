use super::{BT_BINARY, BT_LIST, BinaryInput, JaegerProcess, WireError, read_binary_key_value};

pub(crate) fn read_binary_process(input: &mut BinaryInput<'_>) -> Result<JaegerProcess, WireError> {
    let mut out = JaegerProcess::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_BINARY) => out.service_name = input.read_string()?,
            (2, BT_LIST) => out.tags = input.read_struct_list(read_binary_key_value)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
