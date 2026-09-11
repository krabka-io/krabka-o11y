use super::{IntoResponse, Response, StatusCode};

/// `POST /flush`: force buffered writes out to storage.
///
/// Krabka's distributor holds no chunk buffer. `POST /loki/api/v1/push` awaits
/// the WAL append and answers 204 only once the WAL has taken the records, so
/// at the moment this handler runs everything acknowledged is already durable
/// and there is nothing left to force. The empty 204 is `Loki`'s answer to a
/// flush with nothing to do, and here it is the answer every time.
pub(crate) async fn flush_ingester_chunks() -> Response {
    StatusCode::NO_CONTENT.into_response()
}
