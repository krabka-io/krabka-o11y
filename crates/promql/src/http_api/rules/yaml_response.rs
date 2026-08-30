use super::*;

pub(crate) fn yaml_response(status: StatusCode, value: &impl serde::Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(yaml) => (status, [(header::CONTENT_TYPE, "application/yaml")], yaml).into_response(),
        Err(error) => ApiError::internal(format!("YAML encode failed: {error}")).into_response(),
    }
}
