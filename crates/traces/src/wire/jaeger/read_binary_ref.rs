use super::{BT_I32, BT_I64, BinaryInput, JaegerRef, WireError};

pub(crate) fn read_binary_ref(input: &mut BinaryInput<'_>) -> Result<JaegerRef, WireError> {
    let mut out = JaegerRef::default();
    while let Some(field) = input.read_field()? {
        match field.field_id {
            1 if field.field_type == BT_I32 => out.ref_type = input.read_i32()?,
            2 if field.field_type == BT_I64 => out.trace_id_low = input.read_i64()?,
            3 if field.field_type == BT_I64 => out.trace_id_high = input.read_i64()?,
            4 if field.field_type == BT_I64 => out.span_id = input.read_i64()?,
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
