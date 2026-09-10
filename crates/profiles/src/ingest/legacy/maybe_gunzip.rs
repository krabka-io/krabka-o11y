use super::{ByteSize, ProfilesError, gunzip};

/// The two bytes every gzip member starts with.
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Gunzip a `/ingest` pprof body, or pass it through when it is already plain.
///
/// The door takes both: a raw pprof body arrives gzipped from the SDKs that
/// post one, while a multipart `profile` part is usually written plain.
/// Pyroscope answers either with 200, so the format is decided by the bytes
/// rather than by a header.
///
/// # Errors
/// Returns an error when a gzip body is malformed or expands past `max`.
pub(crate) fn maybe_gunzip(body: &[u8], max: ByteSize) -> Result<Vec<u8>, ProfilesError> {
    if body.starts_with(&GZIP_MAGIC) {
        return gunzip(body, max);
    }
    Ok(body.to_vec())
}
