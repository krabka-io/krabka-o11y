use super::*;

pub(crate) async fn render_diff_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
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
        render_diff_inner(state, headers, query),
    )
    .await
}
