use super::{AttrValue, BT_BINARY, BT_BOOL, BT_DOUBLE, BT_I64, BinaryInput, KeyValue, WireError};

pub(crate) fn read_binary_key_value(input: &mut BinaryInput<'_>) -> Result<KeyValue, WireError> {
    let mut key = String::new();
    let mut value = None;
    while let Some((field_type, field_id)) = input.read_field()? {
        match (field_id, field_type) {
            (1, BT_BINARY) => key = input.read_string()?,
            (3, BT_BINARY) => value = Some(AttrValue::Str(input.read_string()?)),
            (4, BT_DOUBLE) => value = Some(AttrValue::Double(input.read_double()?)),
            (5, BT_BOOL) => value = Some(AttrValue::Bool(input.read_bool()?)),
            (6, BT_I64) => value = Some(AttrValue::Int(input.read_i64()?)),
            (7, BT_BINARY) => value = Some(AttrValue::Bytes(input.read_binary()?)),
            _ => input.skip(field_type)?,
        }
    }
    Ok(KeyValue {
        key,
        value: value.unwrap_or_else(|| AttrValue::Str(String::new())),
    })
}
