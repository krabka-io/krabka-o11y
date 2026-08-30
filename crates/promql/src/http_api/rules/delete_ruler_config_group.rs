use super::{MetricStore, State, Path, Arc, PrometheusApiState, HeaderMap, Response, tenant_from_headers, IntoResponse, StatusCode, ApiError};

pub(crate) async fn delete_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant)
                && let Some(groups) = namespaces.get_mut(&namespace)
            {
                groups.remove(&group_name);
                if groups.is_empty() {
                    namespaces.remove(&namespace);
                }
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}
