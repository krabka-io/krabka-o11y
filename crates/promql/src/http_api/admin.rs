use axum::body::Bytes;

use super::{
    ApiError, Arc, ERASURE_REQUEST_PREFIX, ErasureRequest, Extension, HeaderMap, Index,
    IntoResponse, MetricStore, Principal, PrometheusApiState, Response, State, StatusCode,
    SystemTime, authorized_tenant_from_headers, discovery_window, list_erasure_requests,
    parse_discovery_form, put_erasure_request, selector_matchers,
};

pub(crate) async fn delete_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(erasure_store) = &state.erasure_store else {
        return ApiError::not_found("admin API is disabled").into_response();
    };
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return ApiError::bad_data(error.to_string()).into_response(),
    };
    let params = match parse_discovery_form(&body) {
        Ok(params) if !params.matches.is_empty() => params,
        Ok(_) => return ApiError::bad_data("no match[] parameter provided").into_response(),
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let created_at_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        });
    for selector in &params.matches {
        let matcher_sets = match selector_matchers(selector) {
            Ok(matcher_sets) => matcher_sets,
            Err(error) => return ApiError::from(error).into_response(),
        };
        for matchers in &matcher_sets {
            if let Err(error) = Index::new().resolve(tenant.as_str(), matchers) {
                return ApiError::bad_data(error.to_string()).into_response();
            }
        }
        let request = ErasureRequest::new(
            tenant.as_str(),
            selector,
            matcher_sets,
            window.start_ms.saturating_mul(1_000_000),
            window.end_ms.saturating_mul(1_000_000),
            created_at_ns,
        );
        if let Err(error) =
            put_erasure_request(erasure_store, ERASURE_REQUEST_PREFIX, &request).await
        {
            return ApiError::internal(error.to_string()).into_response();
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn clean_tombstones<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let Some(erasure_store) = &state.erasure_store else {
        return ApiError::not_found("admin API is disabled").into_response();
    };
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return ApiError::bad_data(error.to_string()).into_response(),
    };
    let requests = match list_erasure_requests(erasure_store, ERASURE_REQUEST_PREFIX).await {
        Ok(requests) => requests,
        Err(error) => return ApiError::internal(error.to_string()).into_response(),
    };
    for mut request in requests
        .into_iter()
        .filter(|request| request.tenant == tenant.as_str())
    {
        request.clean_requested = true;
        if let Err(error) =
            put_erasure_request(erasure_store, ERASURE_REQUEST_PREFIX, &request).await
        {
            return ApiError::internal(error.to_string()).into_response();
        }
    }
    StatusCode::NO_CONTENT.into_response()
}
