use super::{
    Arc, CardinalityParams, HeaderMap, IntoResponse, Json, MetricStore, Principal,
    PrometheusApiState, Response, authorized_tenant_from_headers,
    cardinality_label_values_response, cardinality_series,
};

pub(crate) async fn cardinality_label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: CardinalityParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, tenant.as_str(), &params).await {
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
