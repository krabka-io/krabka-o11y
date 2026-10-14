use super::{CompactInput, JaegerBatch, T_LIST, T_STRUCT, WireError, read_process, read_span};

pub(crate) fn read_batch(input: &mut CompactInput<'_>) -> Result<JaegerBatch, WireError> {
    let mut out = JaegerBatch::default();
    let mut last = 0;
    while let Some(field) = input.read_field(&mut last)? {
        match field.field_id {
            1 if field.field_type == T_STRUCT => out.process = read_process(input)?,
            2 if field.field_type == T_LIST => out.spans = input.read_struct_list(read_span)?,
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
