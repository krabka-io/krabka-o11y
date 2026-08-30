use super::*;

pub(crate) async fn ready(Extension(readiness): Extension<ServiceReadiness>) -> Response {
    if readiness.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}
