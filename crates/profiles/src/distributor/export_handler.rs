use super::{
    ConnectError, ConnectIngest, ConnectRequest, ConnectResponse, IngestRequestParts, Message as _,
    connect_ingest, decode_otlp, pb,
};

pub(crate) async fn export_handler(
    IngestRequestParts {
        state,
        principal,
        headers,
    }: IngestRequestParts,
    req: ConnectRequest<pb::otlp_profiles::ExportProfilesServiceRequest>,
) -> Result<ConnectResponse<pb::otlp_profiles::ExportProfilesServiceResponse>, ConnectError> {
    let bytes = req.0.encoded_len() as u64;
    connect_ingest(ConnectIngest {
        state: &state,
        principal: &principal,
        headers: &headers,
        bytes,
        decode: || decode_otlp(&req.0),
    })
    .await?;
    Ok(ConnectResponse::new(
        pb::otlp_profiles::ExportProfilesServiceResponse {
            partial_success: None,
        },
    ))
}
