use axum::response::IntoResponse;
use krabka_observability::server_security::{Principal, authorize_tenant};

use super::{
    Arc, LiveSource, LiveStore, RwLock, TENANT_HEADER, TenantId, TenantPolicy, live_i64_param,
    trace_querier,
};

#[derive(Clone)]
pub(crate) struct LiveStoreState {
    pub(crate) store: Arc<RwLock<LiveStore>>,
    pub(crate) tenant_policy: TenantPolicy,
}

pub(crate) async fn live_span_batches(
    axum::extract::State(state): axum::extract::State<LiveStoreState>,
    axum::extract::Extension(principal): axum::extract::Extension<Principal>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> axum::response::Response {
    let start = match live_i64_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return text_response(axum::http::StatusCode::BAD_REQUEST, err),
    };
    let end = match live_i64_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return text_response(axum::http::StatusCode::BAD_REQUEST, err),
    };
    if end < start {
        return text_response(
            axum::http::StatusCode::BAD_REQUEST,
            "end must be >= start".to_string(),
        );
    }
    // Tempo with multi-tenancy off ignores this header. Krabka does not: the
    // live store holds spans by tenant. So a malformed value is a 400 here,
    // and the read never runs as the fallback tenant.
    let tenant = match TenantId::resolve(
        headers
            .get(TENANT_HEADER)
            .map(axum::http::HeaderValue::as_bytes),
        &state.tenant_policy,
    ) {
        Ok(tenant) => tenant,
        Err(err) => {
            return text_response(axum::http::StatusCode::BAD_REQUEST, err.to_string());
        }
    };
    if let Err(denied) = authorize_tenant(&principal, &tenant) {
        return denied.into_response();
    }
    let guard = state.store.read().await;
    let batches = match guard.span_batches(tenant.as_str(), start, end).await {
        Ok(batches) => batches,
        Err(err) => {
            return text_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            );
        }
    };
    match trace_querier::live::encode_span_batches(&batches) {
        Ok(bytes) => axum::http::Response::builder()
            .header(
                axum::http::header::CONTENT_TYPE,
                "application/vnd.apache.arrow.stream",
            )
            .body(axum::body::Body::from(bytes))
            .expect("valid Arrow response"),
        Err(err) => text_response(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        ),
    }
}

fn text_response(status: axum::http::StatusCode, body: String) -> axum::response::Response {
    axum::http::Response::builder()
        .status(status)
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )
        .body(axum::body::Body::from(body))
        .expect("valid plain text response")
}
