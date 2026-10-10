use super::{
    ConnectError, ConnectIngest, ConnectRequest, ConnectResponse, IngestRequestParts, Message as _,
    connect_ingest, decode_push, pb,
};

pub(crate) async fn push_handler(
    IngestRequestParts {
        state,
        principal,
        headers,
    }: IngestRequestParts,
    req: ConnectRequest<pb::push::v1::PushRequest>,
) -> Result<ConnectResponse<pb::push::v1::PushResponse>, ConnectError> {
    let bytes = req.0.encoded_len() as u64;
    connect_ingest(ConnectIngest {
        state: &state,
        principal: &principal,
        headers: &headers,
        bytes,
        decode: || decode_push(&req.0, state.max_decompressed),
    })
    .await?;
    Ok(ConnectResponse::new(pb::push::v1::PushResponse {}))
}
