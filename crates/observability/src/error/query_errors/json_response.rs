use super::{IntoResponse, Response, StatusCode, Value};

pub(crate) fn json_response(status: StatusCode, value: &Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        value.to_string(),
    )
        .into_response()
}
