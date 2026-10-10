use super::{
    Bytes, HeaderMap, PostedIndexVolumeQuery, QuerierState, RawQuery, RequestSecurity, Response,
    State, VolumeKind, posted_index_volume_response,
};

pub(crate) async fn index_volume_range_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    posted_index_volume_response(
        &state,
        PostedIndexVolumeQuery {
            security: &security,
            headers: &headers,
            raw_query: raw_query.as_deref(),
            body: &body,
            kind: VolumeKind::Range,
        },
    )
    .await
}
