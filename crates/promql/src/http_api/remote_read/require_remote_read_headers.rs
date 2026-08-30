use super::{HeaderMap, ApiError, header, StatusCode, header_list_includes};

pub(crate) fn require_remote_read_headers(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    if !content_type.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "remote_read requires application/x-protobuf".into(),
        });
    }

    let content_encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim();
    if !header_list_includes(content_encoding, "snappy") {
        return Err(ApiError::bad_data(
            "remote_read requires snappy content encoding",
        ));
    }
    Ok(())
}
