use super::WireError;

/// Decode a thrift string's bytes, which must be UTF-8.
pub(crate) fn utf8_string(bytes: Vec<u8>) -> Result<String, WireError> {
    String::from_utf8(bytes).map_err(|err| WireError::Decode(err.to_string()))
}

pub(crate) fn read_utf8_binary(
    read_binary: impl FnOnce() -> Result<Vec<u8>, WireError>,
) -> Result<String, WireError> {
    utf8_string(read_binary()?)
}
