use super::*;

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_read_response(response: &v1::ReadResponse) -> Result<Vec<u8>, RemoteReadError> {
    let mut raw = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut raw)
        .map_err(|error| RemoteReadError::Encode(error.to_string()))?;
    snap::raw::Encoder::new()
        .compress_vec(&raw)
        .map_err(|error| RemoteReadError::SnappyEncode(error.to_string()))
}
