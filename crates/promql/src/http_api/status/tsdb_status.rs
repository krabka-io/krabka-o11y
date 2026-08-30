use super::*;

pub(crate) async fn tsdb_status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_tsdb_status_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_stats(&tenant).await {
        Ok(tsdb) => success_data_response(tsdb_status_json(tsdb, params.limit)),
        Err(error) => ApiError::from(error).into_response(),
    }
}
