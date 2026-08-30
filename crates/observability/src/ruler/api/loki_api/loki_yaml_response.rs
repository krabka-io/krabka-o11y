use super::*;

pub(crate) fn loki_yaml_response(status: StatusCode, value: &impl Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(body) => (
            status,
            [("content-type", "application/yaml; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(source) => text_response(StatusCode::INTERNAL_SERVER_ERROR, &source.to_string()),
    }
}
