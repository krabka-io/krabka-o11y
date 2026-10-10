use super::{Path, QuerierRequest, Response, SpanStore, search_tag_values_json, timed_tag_values};

pub(crate) async fn search_tag_values<S>(
    request: QuerierRequest<S>,
    Path(tag): Path<String>,
) -> Response
where
    S: SpanStore + 'static,
{
    timed_tag_values(request, tag, search_tag_values_json).await
}
