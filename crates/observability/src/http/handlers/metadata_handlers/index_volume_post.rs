use super::{
    HttpQueryError, PostedQueryRequest, QuerierState, Response, State, VolumeKind,
    posted_index_volume_response,
};

pub(crate) async fn index_volume_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    posted_index_volume_response(&state, &request, VolumeKind::Instant).await
}
