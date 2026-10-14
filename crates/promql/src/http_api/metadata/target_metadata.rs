use super::{
    Arc, MetadataRepresentation, MetricStore, PrometheusApiState, RawQuery, RequestCaller,
    Response, State, metadata_response_for_representation,
};

pub(crate) async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
) -> Response {
    metadata_response_for_representation(state, caller, raw_query, MetadataRepresentation::Targets)
        .await
}
