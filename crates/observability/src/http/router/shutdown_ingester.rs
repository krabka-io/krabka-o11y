use super::{IntoResponse, Response, StatusCode};

pub(crate) async fn shutdown_ingester() -> Response {
    StatusCode::NO_CONTENT.into_response()
}
