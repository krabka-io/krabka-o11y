use super::{IntoResponse, Response, StatusCode};

pub(crate) fn text_response(status: StatusCode, value: &str) -> Response {
    (
        status,
        [("content-type", "text/plain; charset=utf-8")],
        value.to_string(),
    )
        .into_response()
}
