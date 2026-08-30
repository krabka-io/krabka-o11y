use super::*;

pub(crate) fn require_otlp_protobuf_content_type(headers: &HeaderMap) -> Result<(), WireError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let base = content_type.split(';').next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(WireError::UnsupportedContentType(base.to_string()));
    }
    Ok(())
}
