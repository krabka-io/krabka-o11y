use super::*;

pub(crate) fn span_id_part(bytes: &[u8]) -> Result<i64, WireError> {
    if bytes.len() != 8 {
        return Err(WireError::Decode("jaeger span_id must be 8 bytes".into()));
    }
    Ok(i64::from_be_bytes(
        bytes[0..8].try_into().expect("slice length checked"),
    ))
}
