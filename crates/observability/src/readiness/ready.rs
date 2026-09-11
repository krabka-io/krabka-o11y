use super::{Extension, IntoResponse, Response, RoleReadiness, StatusCode};

pub(crate) async fn ready(Extension(readiness): Extension<RoleReadiness>) -> Response {
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
