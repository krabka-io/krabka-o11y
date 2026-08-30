use super::*;

pub(crate) async fn render_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    query: Query<RenderQuery>,
) -> Response
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query_response(&metrics, "render", render_inner(state, headers, query)).await
}
