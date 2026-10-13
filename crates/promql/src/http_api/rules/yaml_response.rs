use super::{ApiError, IntoResponse, Response, StatusCode, header, response_with_status};

pub(crate) fn yaml_response(status: StatusCode, value: &impl serde::Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(yaml) => {
            let mut response = response_with_status(status, yaml);
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/yaml"),
            );
            response
        }
        Err(error) => ApiError::internal(format!("YAML encode failed: {error}")).into_response(),
    }
}
