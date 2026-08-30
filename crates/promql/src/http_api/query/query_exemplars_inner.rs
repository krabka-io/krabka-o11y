use super::*;

pub(crate) async fn query_exemplars_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: ExemplarsQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match selector_matchers(&params.query) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let start_ms = match timestamp_ms(&params.start) {
        Ok(start_ms) => start_ms,
        Err(error) => return error.into_response(),
    };
    let end_ms = match timestamp_ms(&params.end) {
        Ok(end_ms) => end_ms,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_timestamp_range(start_ms, end_ms) {
        return error.into_response();
    }

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .exemplars(&tenant, &matchers, start_ms, end_ms)
            .await
        {
            Ok(exemplars) => {
                for exemplar in exemplars {
                    by_key.insert(exemplar_key(&exemplar), exemplar);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    success_data_response(exemplars_json(by_key.into_values().collect()))
}
