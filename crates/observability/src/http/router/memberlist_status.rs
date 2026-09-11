use super::{IntoResponse, Response, StatusCode};

/// The memberlist page.
///
/// Kept as a constant deliberately, and it is not a stub: Krabka joins no
/// gossip cluster, and this is the sentence real `Loki` serves here when its
/// memberlist KV is not configured. Anything else -- a peer table, a "healthy"
/// claim -- would be the fabrication this endpoint is supposed to avoid.
pub(crate) async fn memberlist_status() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "This instance doesn't use memberlist.",
    )
        .into_response()
}
