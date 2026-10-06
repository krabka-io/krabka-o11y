use axum::body::Bytes;

use super::{
    ApiError, Arc, ERASURE_REQUEST_PREFIX, ErasureRequest, Extension, HeaderMap, IntoResponse,
    MetricStore, Principal, PrometheusApiState, Response, State, StatusCode, SystemTime,
    authorize_admin, authorized_tenant_from_headers, discovery_window, parse_discovery_form,
    put_erasure_request, selector_matchers,
};

pub(crate) async fn delete_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
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
    let mut requests = Vec::with_capacity(params.matches.len());
    for selector in &params.matches {
        let typed_matcher_sets = match selector_matchers(selector) {
            Ok(matcher_sets) => matcher_sets,
            Err(error) => return ApiError::from(error).into_response(),
        };
        for matchers in &typed_matcher_sets {
            if let Err(error) = crate::in_memory::prepare_matchers(matchers) {
                return ApiError::bad_data(error.to_string()).into_response();
            }
        }
        requests.push((selector, typed_matcher_sets));
    }
    let created_at_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        });
    for (selector, matcher_sets) in requests {
        let mut request = ErasureRequest::new(
            tenant.as_str(),
            selector,
            matcher_sets
                .iter()
                .map(|set| {
                    set.iter()
                        .filter_map(crate::PromqlMatcher::index_matcher)
                        .collect()
                })
                .collect(),
            window.start_ms.saturating_mul(1_000_000),
            window.end_ms.saturating_mul(1_000_000),
            created_at_ns,
        );
        if matcher_sets
            .iter()
            .flatten()
            .any(|matcher| matcher.value.utf8().is_none())
        {
            request = request.with_byte_matchers(
                matcher_sets
                    .iter()
                    .map(|set| {
                        set.iter()
                            .map(|matcher| krabka_blockstore::ByteLabelMatcher {
                                name: matcher.name.clone(),
                                op: matcher.op,
                                value: matcher.value.as_bytes().to_vec(),
                            })
                            .collect()
                    })
                    .collect(),
            );
        }
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
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
    if state.erasure_store.is_none() {
        return ApiError::not_found("admin API is disabled").into_response();
    }
    match authorized_tenant_from_headers(&headers, &principal) {
        Ok(_) => {}
        Err(error) => return ApiError::bad_data(error.to_string()).into_response(),
    }
    // Durable requests stay active until a WAL publication watermark exists;
    // retiring one here could let unflushed matching samples reappear.
    StatusCode::NO_CONTENT.into_response()
}
