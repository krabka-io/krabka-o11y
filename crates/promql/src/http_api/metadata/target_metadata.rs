use super::{
    Arc, MetricStore, PrometheusApiState, RawQuery, RequestCaller, Response, State,
    metadata_response, success_data_response, target_metadata_json,
};

pub(crate) async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
) -> Response {
    metadata_response(
        &state,
        caller.auth(),
        raw_query.as_deref(),
        |_, metadata| success_data_response(target_metadata_json(metadata)),
    )
    .await
}
