use super::{
    Arc, CardinalityParams, HeaderMap, IntoResponse, Json, MetricStore, Principal,
    PrometheusApiState, Response, active_series_response, apply_limit,
    authorized_tenant_from_headers, cardinality_series, enforce_selected_series_limit,
};

pub(crate) async fn cardinality_active_series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: CardinalityParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = cardinality_series(&state, tenant.as_str(), &params).await;
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
