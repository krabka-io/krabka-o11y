use super::{IntoResponse, Response, StatusCode};

pub(crate) async fn memberlist_status() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "This instance doesn't use memberlist.",
    )
        .into_response()
}
