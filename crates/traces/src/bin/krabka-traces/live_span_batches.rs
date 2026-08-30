use super::{Arc, RwLock, LiveStore, live_i64_param, LiveSource, trace_querier};

pub(crate) async fn live_span_batches(
    axum::extract::State(live_store): axum::extract::State<Arc<RwLock<LiveStore>>>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let start = match live_i64_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (axum::http::StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end = match live_i64_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (axum::http::StatusCode::BAD_REQUEST, err).into_response(),
    };
    if end < start {
        return (axum::http::StatusCode::BAD_REQUEST, "end must be >= start").into_response();
    }
    let tenant = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous");
    let guard = live_store.read().await;
    let batches = match guard.span_batches(tenant, start, end).await {
        Ok(batches) => batches,
        Err(err) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )
                .into_response();
        }
    };
    match trace_querier::live::encode_span_batches(&batches) {
        Ok(bytes) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/vnd.apache.arrow.stream",
            )],
            bytes,
        )
            .into_response(),
        Err(err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        )
            .into_response(),
    }
}
