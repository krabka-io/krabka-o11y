use super::{
    Arc, CardinalityParams, IntoResponse, Json, MetricStore, PrometheusApiState, RequestAuth,
    Response, authorized_cardinality_series, cardinality_label_values_response,
};

pub(crate) async fn cardinality_label_values_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: CardinalityParams,
) -> Response {
    let (_, series) = match authorized_cardinality_series(state, auth, &params).await {
        Ok(selected) => selected,
        Err(rejection) => return rejection.into_response(),
    };
    Json(cardinality_label_values_response(
        &series,
        &params.label_names,
        params.limit,
    ))
    .into_response()
}
