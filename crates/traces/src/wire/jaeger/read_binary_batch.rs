use super::{
    BT_LIST, BT_STRUCT, BinaryInput, JaegerBatch, WireError, read_binary_process, read_binary_span,
};

pub(crate) fn read_binary_batch(input: &mut BinaryInput<'_>) -> Result<JaegerBatch, WireError> {
    let mut out = JaegerBatch::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_STRUCT) => out.process = read_binary_process(input)?,
            (2, BT_LIST) => out.spans = input.read_struct_list(read_binary_span)?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
