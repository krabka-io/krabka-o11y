use super::{IntoResponse, Response, StatusCode};

pub(crate) async fn flush_ingester_chunks() -> Response {
    StatusCode::NO_CONTENT.into_response()
}
