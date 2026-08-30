use super::{CompactInput, JaegerRef, T_I32, T_I64, WireError};

pub(crate) fn read_ref(input: &mut CompactInput<'_>) -> Result<JaegerRef, WireError> {
    let mut out = JaegerRef::default();
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_I32) => out.ref_type = input.read_i32()?,
            (2, T_I64) => out.trace_id_low = input.read_i64()?,
            (3, T_I64) => out.trace_id_high = input.read_i64()?,
            (4, T_I64) => out.span_id = input.read_i64()?,
            _ => input.skip(field_type)?,
        }
    }
    Ok(out)
}
