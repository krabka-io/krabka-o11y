use super::*;

pub(crate) const CONTENT_ENCODING: &str = "content-encoding";

pub(crate) fn decode_body(
    headers: &HeaderMap,
    body: &[u8],
    max_decompressed: ByteSize,
) -> Result<Vec<u8>, TracesError> {
    let max_decompressed = max_decompressed.bytes_usize();
    let encoding = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("identity");
    let decoded = if encoding.eq_ignore_ascii_case("identity") {
        body.to_vec()
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut out = Vec::new();
        GzDecoder::new(body)
            .take(u64::try_from(max_decompressed).unwrap_or(u64::MAX) + 1)
            .read_to_end(&mut out)
            .map_err(|err| TracesError::Decode(err.to_string()))?;
        out
    } else {
        return Err(TracesError::UnsupportedContentType(encoding.to_string()));
    };
    if decoded.len() > max_decompressed {
        return Err(TracesError::TooLarge {
            limit: max_decompressed,
        });
    }
    Ok(decoded)
}

pub(crate) fn require_content_type(
    headers: &HeaderMap,
    allowed: &[&str],
) -> Result<(), TracesError> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Ok(());
    };
    let declared = value
        .to_str()
        .map_err(|err| TracesError::UnsupportedContentType(err.to_string()))?;
    let media_type = declared
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if allowed
        .iter()
        .any(|allowed| media_type == allowed.to_ascii_lowercase())
    {
        Ok(())
    } else {
        Err(TracesError::UnsupportedContentType(declared.to_string()))
    }
}
