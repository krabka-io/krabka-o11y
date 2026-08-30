use super::*;

pub(crate) async fn loki_page_not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "404 page not found\n")
}
