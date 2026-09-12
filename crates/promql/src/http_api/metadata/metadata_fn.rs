use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    RawQuery, Response, State, apply_limit, authorized_tenant_from_headers, metadata_json,
    parse_metadata_params, success_data_response,
};

pub(crate) async fn metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_metadata_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state
        .store
        .metadata(tenant.as_str(), params.metric.as_deref())
        .await
    {
        Ok(scan) => {
            let mut metadata = scan.metadata;
            apply_limit(&mut metadata, params.limit);
            success_data_response(metadata_json(metadata, params.limit_per_metric))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}
