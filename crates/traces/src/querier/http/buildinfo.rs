use super::*;

/// Tempo-compatible build info.
///
/// Grafana's Tempo datasource probes this on every query to detect the backend
/// version. Without it, Grafana treats the backend as a legacy Tempo and falls
/// back to endpoints this crate does not serve, which breaks the trace-by-id
/// view. The Prometheus-style `{status, data:{version,...}}` shape matches
/// Tempo's `/api/status/buildinfo`.
pub(crate) async fn buildinfo() -> Response {
    Json(json!({
        "status": "success",
        "data": {
            "version": "2.6.0",
            "revision": "krabka",
            "branch": "main",
            "buildUser": "krabka",
            "buildDate": "",
            "goVersion": "",
        },
    }))
    .into_response()
}
