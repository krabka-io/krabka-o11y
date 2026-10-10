use super::{
    Arc, IntoResponse, MetricStore, PrometheusApiState, RawQuery, RequestCaller, Response, State,
    limited_metadata, success_data_response, target_metadata_json,
};

pub(crate) async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match limited_metadata(&state, caller.auth(), raw_query.as_deref()).await {
        Ok((_, metadata)) => success_data_response(target_metadata_json(metadata)),
        Err(rejection) => rejection.into_response(),
    }
}
