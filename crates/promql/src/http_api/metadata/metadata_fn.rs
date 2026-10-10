use super::{
    Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState, RawQuery,
    RequestAuth, Response, State, limited_metadata, metadata_json, success_data_response,
};

pub(crate) async fn metadata<S: MetricStore>(
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
        Ok((metadata_params, metadata)) => {
            success_data_response(metadata_json(metadata, metadata_params.limit_per_metric))
        }
        Err(rejection) => rejection.into_response(),
    }
}
