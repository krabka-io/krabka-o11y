use super::*;

// cargo-mutants: covered by remote_read decode round-trip and snappy limit tests.
#[cfg_attr(test, mutants::skip)]
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_read_request(
    snappy_body: &[u8],
    max_output: ByteSize,
) -> Result<v1::ReadRequest, RemoteReadError> {
    let raw = snappy_block_decode_raw(
        snappy_body,
        max_output.bytes_usize(),
        RemoteReadError::SnappyDecode,
        RemoteReadError::SnappyOutputTooLarge,
    )?;
    v1::ReadRequest::decode(raw.as_slice())
        .map_err(|error| RemoteReadError::Decode(error.to_string()))
}
