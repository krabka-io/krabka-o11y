use super::*;

/// Gunzip a gzipped body with an output-size cap.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn gunzip(body: &[u8], max_output: ByteSize) -> Result<Vec<u8>, ProfilesError> {
    // The read loop compares against buffer lengths, so the cap crosses into
    // its exact byte count here.
    let max_output = max_output.bytes_usize();
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut out = Vec::new();
    let mut buf = [0_u8; 8192];

    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| ProfilesError::Gunzip(e.to_string()))?;
        if n == 0 {
            break;
        }
        if out.len() + n > max_output {
            return Err(ProfilesError::TooLarge { limit: max_output });
        }
        out.extend_from_slice(&buf[..n]);
    }

    Ok(out)
}
