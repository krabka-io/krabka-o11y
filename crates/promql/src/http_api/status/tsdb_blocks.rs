use super::{
    ApiError, Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState,
    Response, State, authorized_tenant_from_headers, json, success_data_response, tsdb_blocks_json,
};

pub(crate) async fn tsdb_blocks<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_blocks(tenant.as_str()).await {
        Ok(blocks) => success_data_response(json!({
            "blocks": tsdb_blocks_json(blocks),
        })),
        Err(error) => ApiError::from(error).into_response(),
    }
}
