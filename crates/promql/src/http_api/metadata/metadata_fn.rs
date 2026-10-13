use super::{
    Arc, MetricStore, PrometheusApiState, RawQuery, RequestCaller, Response, State, metadata_json,
    metadata_response, success_data_response,
};

pub(crate) async fn metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
) -> Response {
    metadata_response(
        &state,
        caller.auth(),
        raw_query.as_deref(),
        |params, metadata| success_data_response(metadata_json(metadata, params.limit_per_metric)),
    )
    .await
}
