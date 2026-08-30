use super::{ApiError, Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, Response, State, json, success_data_response, tenant_from_headers, tsdb_blocks_json};

pub(crate) async fn tsdb_blocks<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_blocks(&tenant).await {
        Ok(blocks) => success_data_response(json!({
            "blocks": tsdb_blocks_json(blocks),
        })),
        Err(error) => ApiError::from(error).into_response(),
    }
}
