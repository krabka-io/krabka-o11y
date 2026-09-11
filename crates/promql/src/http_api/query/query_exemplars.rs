use super::{
    Arc, Extension, HeaderMap, IntoResponse, MetricStore, Principal, PrometheusApiState, RawQuery,
    Response, State, exemplars_query_params_from_form, query_exemplars_inner,
};

pub(crate) async fn query_exemplars<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match exemplars_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes())
        {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_exemplars_inner(state, headers, principal, params).await
}
