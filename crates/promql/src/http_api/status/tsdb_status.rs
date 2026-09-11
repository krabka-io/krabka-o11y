use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RawQuery, Response, State, authorized_tenant_from_headers, parse_tsdb_status_params,
    success_data_response, tsdb_status_json,
};

pub(crate) async fn tsdb_status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_tsdb_status_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_stats(tenant.as_str()).await {
        Ok(tsdb) => success_data_response(tsdb_status_json(tsdb, params.limit)),
        Err(error) => ApiError::from(error).into_response(),
    }
}
