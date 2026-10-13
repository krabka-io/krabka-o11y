use super::{Response, StatusCode};

pub(crate) fn text_response(status: StatusCode, value: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(value.to_string().into())
        .expect("text response headers are valid")
}
