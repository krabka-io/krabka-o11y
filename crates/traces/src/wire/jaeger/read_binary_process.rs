use super::{
    BT_BINARY, BT_LIST, BinaryInput, JaegerProcess, KeyValueReader, WireError,
    read_binary_key_value,
};

pub(crate) fn read_binary_process(input: &mut BinaryInput<'_>) -> Result<JaegerProcess, WireError> {
    let mut out = JaegerProcess::default();
    while let Some(field) = input.read_field()? {
        match field.field_id {
            1 if field.field_type == BT_BINARY => out.service_name = input.read_string()?,
            2 if field.field_type == BT_LIST => {
                out.tags = input.read_struct_list(read_binary_key_value)?
            }
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
