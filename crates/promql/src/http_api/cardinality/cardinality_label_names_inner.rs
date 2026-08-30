use super::{MetricStore, Arc, PrometheusApiState, HeaderMap, CardinalityParams, Response, tenant_from_headers, IntoResponse, cardinality_series, Json, cardinality_label_names_response};

pub(crate) async fn cardinality_label_names_inner<S: MetricStore>(
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
    Json(cardinality_label_names_response(&series, params.limit)).into_response()
}
