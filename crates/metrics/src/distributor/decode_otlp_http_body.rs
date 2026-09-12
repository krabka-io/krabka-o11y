use std::io::Read as _;

use axum::http::{HeaderMap, header::CONTENT_ENCODING};
use flate2::read::GzDecoder;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use crate::wire::WireError;

pub(crate) fn decode_otlp_http_body(
    headers: &HeaderMap,
    body: &[u8],
    max_decompressed: ByteSize,
) -> Result<Vec<u8>, WireError> {
    let limit = max_decompressed.bytes_usize();
    let encoding = match headers.get(CONTENT_ENCODING) {
        None => "identity",
        Some(value) => value
            .to_str()
            .map_err(|_| WireError::UnsupportedContentEncoding("non-UTF-8".into()))?,
    };
    if encoding.eq_ignore_ascii_case("identity") {
        if body.len() > limit {
            return Err(WireError::DecodedBodyTooLarge(limit));
        }
        return Ok(body.to_vec());
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return Err(WireError::UnsupportedContentEncoding(encoding.to_string()));
    }

    let mut decoded = Vec::new();
    GzDecoder::new(body)
        .take(u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| WireError::GzipDecode(error.to_string()))?;
    if decoded.len() > limit {
        return Err(WireError::DecodedBodyTooLarge(limit));
    }
    Ok(decoded)
}
