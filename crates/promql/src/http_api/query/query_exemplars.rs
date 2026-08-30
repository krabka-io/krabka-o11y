use super::{
    Arc, HeaderMap, IntoResponse, MetricStore, PrometheusApiState, RawQuery, Response, State,
    exemplars_query_params_from_form, query_exemplars_inner,
};

pub(crate) async fn query_exemplars<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match exemplars_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes())
        {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_exemplars_inner(state, headers, params).await
}
