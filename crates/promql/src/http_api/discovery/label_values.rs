use super::{
    Arc, Extension, HeaderMap, IntoResponse, LabelValuesQuery, MetricStore, Path, Principal,
    PrometheusApiState, RawQuery, RequestAuth, Response, State, label_values_inner,
    parse_discovery_params,
};

pub(crate) async fn label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        LabelValuesQuery { name, params },
    )
    .await
}
