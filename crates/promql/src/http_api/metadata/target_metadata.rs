use super::{ApiError, Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, RawQuery, Response, State, apply_limit, parse_metadata_params, success_data_response, target_metadata_json, tenant_from_headers};

pub(crate) async fn target_metadata<S: MetricStore>(
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
            success_data_response(target_metadata_json(metadata))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
