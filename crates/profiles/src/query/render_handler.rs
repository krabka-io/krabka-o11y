use super::{
    Arc, Extension, HeaderMap, Principal, ProfileStore, QuerierState, Query, RenderQuery, Response,
    render_inner, timed_query_response,
};

pub(crate) async fn render_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    query: Query<RenderQuery>,
) -> Response
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query_response(
        &metrics,
        "render",
        render_inner(state, principal, headers, query),
    )
    .await
}
