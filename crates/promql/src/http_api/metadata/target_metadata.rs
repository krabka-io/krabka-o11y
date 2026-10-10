use super::{
    Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState, RawQuery,
    RequestAuth, Response, State, limited_metadata, success_data_response, target_metadata_json,
};

pub(crate) async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match limited_metadata(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        raw_query.as_deref(),
    )
    .await
    {
        Ok((_, metadata)) => success_data_response(target_metadata_json(metadata)),
        Err(rejection) => rejection.into_response(),
    }
}
