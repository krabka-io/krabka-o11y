use super::{HeaderMap, WireError, header_list_includes};

pub(crate) fn require_snappy_encoding(headers: &HeaderMap) -> Result<(), WireError> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !header_list_includes(encoding, "snappy") {
        return Err(WireError::UnsupportedContentEncoding(encoding.to_string()));
    }
    Ok(())
}
