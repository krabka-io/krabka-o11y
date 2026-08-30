use super::*;

/// Decodes a plain snappy block. Prometheus `remote_write` does not use the
/// Xerial framed snappy format that Kafka uses.
///
/// This function checks the block's stored uncompressed length against
/// `max_output` *before* it decompresses, so it rejects a decompression bomb
/// before `snap` pre-allocates the declared buffer. Such a bomb is a tiny
/// payload that declares a huge length.
// cargo-mutants: covered by remote-write snappy round-trip and limit tests.
#[cfg_attr(test, mutants::skip)]
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn snappy_block_decode(body: &[u8], max_output: ByteSize) -> Result<Vec<u8>, WireError> {
    snappy_block_decode_raw(
        body,
        max_output.bytes_usize(),
        WireError::SnappyDecode,
        WireError::SnappyOutputTooLarge,
    )
}
