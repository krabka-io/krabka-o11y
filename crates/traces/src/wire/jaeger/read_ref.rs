use super::{CompactInput, JaegerRef, T_I32, T_I64, WireError};

pub(crate) fn read_ref(input: &mut CompactInput<'_>) -> Result<JaegerRef, WireError> {
    let mut out = JaegerRef::default();
    let mut last = 0;
    while let Some(field) = input.read_field(&mut last)? {
        match field.field_id {
            1 if field.field_type == T_I32 => out.ref_type = input.read_i32()?,
            2 if field.field_type == T_I64 => out.trace_id_low = input.read_i64()?,
            3 if field.field_type == T_I64 => out.trace_id_high = input.read_i64()?,
            4 if field.field_type == T_I64 => out.span_id = input.read_i64()?,
            _ => input.skip(field.field_type)?,
        }
    }
    Ok(out)
}
