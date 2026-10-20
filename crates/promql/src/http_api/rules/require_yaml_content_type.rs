use super::{ApiError, HeaderMap, StatusCode};

pub(crate) fn require_yaml_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = super::super::request_content_type(headers);
    match content_type {
        "application/yaml" | "application/x-yaml" | "text/yaml" => Ok(()),
        _ => Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "ruler config requires application/yaml".into(),
        }),
    }
}
