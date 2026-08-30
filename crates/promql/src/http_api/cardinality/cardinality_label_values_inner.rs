use super::*;

pub(crate) async fn cardinality_label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, &tenant, &params).await {
        Ok(series) => series,
        Err(error) => return error.into_response(),
    };
    Json(cardinality_label_values_response(
        &series,
        &params.label_names,
        params.limit,
    ))
    .into_response()
}
