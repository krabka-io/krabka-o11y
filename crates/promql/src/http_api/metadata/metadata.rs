use super::{ApiError, Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, RawQuery, Response, State, apply_limit, metadata_json, parse_metadata_params, success_data_response, tenant_from_headers};

pub(crate) async fn metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_metadata_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state
        .store
        .metadata(&tenant, params.metric.as_deref())
        .await
    {
        Ok(mut metadata) => {
            apply_limit(&mut metadata, params.limit);
            success_data_response(metadata_json(metadata, params.limit_per_metric))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
