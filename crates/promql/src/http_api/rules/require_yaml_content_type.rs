use super::{ApiError, HeaderMap, StatusCode, header};

pub(crate) fn require_yaml_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    match content_type {
        "application/yaml" | "application/x-yaml" | "text/yaml" => Ok(()),
        _ => Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "ruler config requires application/yaml".into(),
        }),
    }
}
