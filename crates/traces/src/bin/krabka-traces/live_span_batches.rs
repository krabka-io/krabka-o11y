use krabka_observability::server_security::{Principal, authorize_tenant};

use super::{
    Arc, LiveSource, LiveStore, RwLock, TENANT_HEADER, TenantId, TenantPolicy, live_i64_param,
    trace_querier,
};

pub(crate) async fn live_span_batches(
    axum::extract::State((live_store, tenant_policy)): axum::extract::State<(
        Arc<RwLock<LiveStore>>,
        TenantPolicy,
    )>,
    axum::extract::Extension(principal): axum::extract::Extension<Principal>,
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
    // Tempo with multi-tenancy off ignores this header. Krabka does not: the
    // live store holds spans by tenant. So a malformed value is a 400 here,
    // and the read never runs as the fallback tenant.
    let tenant = match TenantId::resolve(
        headers
            .get(TENANT_HEADER)
            .map(axum::http::HeaderValue::as_bytes),
        &tenant_policy,
    ) {
        Ok(tenant) => tenant,
        Err(err) => {
            return (axum::http::StatusCode::BAD_REQUEST, err.to_string()).into_response();
        }
    };
    if let Err(denied) = authorize_tenant(&principal, &tenant) {
        return denied.into_response();
    }
    let guard = live_store.read().await;
    let batches = match guard.span_batches(tenant.as_str(), start, end).await {
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
