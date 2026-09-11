use super::{Extension, IntoResponse, Response, RoleReadiness, StatusCode};

/// The `GET /ready` handler every Krabka role serves.
///
/// It answers `200` with `ready\n` when every gate of the [`RoleReadiness`] in
/// the request extensions is met, and `503` with `not ready: <name>, <name>\n`
/// when some are not.
///
/// [`readiness_router`](super::readiness_router) mounts it at `/ready`. Mount
/// the handler directly when a role also needs it at a second path, as Tempo
/// does with `/status`. The response format lives here and nowhere else, so a
/// probe that reads the gate names has one format to parse.
pub async fn ready(Extension(readiness): Extension<RoleReadiness>) -> Response {
    let pending = readiness.pending();
    if pending.is_empty() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        // Loki, Mimir and Tempo all name what is still starting in the 503
        // body. An operator reading a probe failure wants the same answer.
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {}\n", pending.join(", ")),
        )
            .into_response()
    }
}
