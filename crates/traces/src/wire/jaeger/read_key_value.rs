use super::{AttrValue, CompactInput, KeyValue, T_BINARY, T_BOOL_FALSE, T_BOOL_TRUE, T_DOUBLE, T_I64, WireError};

pub(crate) fn read_key_value(input: &mut CompactInput<'_>) -> Result<KeyValue, WireError> {
    let mut key = String::new();
    let mut value = None;
    let mut last = 0;
    while let Some((field_type, field_id)) = input.read_field(&mut last)? {
        match (field_id, field_type) {
            (1, T_BINARY) => key = input.read_string()?,
            (3, T_BINARY) => value = Some(AttrValue::Str(input.read_string()?)),
            (4, T_DOUBLE) => value = Some(AttrValue::Double(input.read_double()?)),
            (5, T_BOOL_TRUE) => value = Some(AttrValue::Bool(true)),
            (5, T_BOOL_FALSE) => value = Some(AttrValue::Bool(false)),
            (6, T_I64) => value = Some(AttrValue::Int(input.read_i64()?)),
            (7, T_BINARY) => value = Some(AttrValue::Bytes(input.read_binary()?)),
            _ => input.skip(field_type)?,
        }
    }
    Ok(KeyValue {
        key,
        value: value.unwrap_or_else(|| AttrValue::Str(String::new())),
    })
}
