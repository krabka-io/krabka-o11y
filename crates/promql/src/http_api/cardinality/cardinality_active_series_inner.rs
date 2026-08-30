use super::{
    Arc, CardinalityParams, HeaderMap, IntoResponse, Json, MetricStore, PrometheusApiState,
    Response, active_series_response, apply_limit, cardinality_series,
    enforce_selected_series_limit, tenant_from_headers,
};

pub(crate) async fn cardinality_active_series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = cardinality_series(&state, &tenant, &params).await;
    match series {
        Ok(mut series) => {
            if let Err(error) = enforce_selected_series_limit(&state, &tenant, series.len()) {
                return error.into_response();
            }
            apply_limit(&mut series, params.limit);
            Json(active_series_response(series)).into_response()
        }
        Err(error) => error.into_response(),
    }
}
