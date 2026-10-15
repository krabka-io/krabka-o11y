use super::{
    Arc, Extension, HeaderMap, Principal, ProfileStore, QuerierState, Query, RawQuery, RenderQuery,
    Response, render_diff_inner, render_inner, timed_query_response,
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

pub(crate) async fn render_diff_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    query: RawQuery,
) -> Response
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query_response(
        &metrics,
        "render_diff",
        render_diff_inner(state, principal, headers, query),
    )
    .await
}
