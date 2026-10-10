use super::{
    ApiError, Arc, AuthorizedTenant, IntoResponse, MetricStore, PrometheusApiState, Response,
    State, json, success_data_response, tsdb_blocks_json,
};

pub(crate) async fn tsdb_blocks<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    AuthorizedTenant(tenant): AuthorizedTenant,
) -> Response {
    match state.store.tsdb_blocks(tenant.as_str()).await {
        Ok(blocks) => success_data_response(json!({
            "blocks": tsdb_blocks_json(blocks),
        })),
        Err(error) => ApiError::from(error).into_response(),
    }
}
