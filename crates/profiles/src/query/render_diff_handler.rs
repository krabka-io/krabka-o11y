use super::{
    Arc, Extension, HeaderMap, Principal, ProfileStore, QuerierState, RawQuery, Response,
    render_diff_inner, timed_query_response,
};

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
