use super::{BT_I32, BT_I64, BinaryInput, JaegerRef, WireError};

pub(crate) fn read_binary_ref(input: &mut BinaryInput<'_>) -> Result<JaegerRef, WireError> {
    let mut out = JaegerRef::default();
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_I32) => out.ref_type = input.read_i32()?,
            (2, BT_I64) => out.trace_id_low = input.read_i64()?,
            (3, BT_I64) => out.trace_id_high = input.read_i64()?,
            (4, BT_I64) => out.span_id = input.read_i64()?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
