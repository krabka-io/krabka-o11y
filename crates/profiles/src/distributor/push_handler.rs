use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, DistributorState, Extension, HeaderMap,
    Message as _, Principal, connect_ingest, decode_push, pb,
};

pub(crate) async fn push_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::push::v1::PushRequest>,
) -> Result<ConnectResponse<pb::push::v1::PushResponse>, ConnectError> {
    let bytes = req.0.encoded_len() as u64;
    connect_ingest(&state, &principal, &headers, bytes, || {
        decode_push(&req.0, state.max_decompressed)
    })
    .await?;
    Ok(ConnectResponse::new(pb::push::v1::PushResponse {}))
}
