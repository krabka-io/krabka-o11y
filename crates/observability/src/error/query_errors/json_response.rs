use super::{Response, StatusCode, Value};

pub(crate) fn json_response(status: StatusCode, value: &Value) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(value.to_string().into())
        .expect("JSON response headers are valid")
}
