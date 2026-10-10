use super::{
    Arc, ConnectError, ConnectRequest, ConnectResponse, DistributorState, Extension, HeaderMap,
    Message as _, Principal, connect_ingest, decode_otlp, pb,
};

pub(crate) async fn export_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::otlp_profiles::ExportProfilesServiceRequest>,
) -> Result<ConnectResponse<pb::otlp_profiles::ExportProfilesServiceResponse>, ConnectError> {
    let bytes = req.0.encoded_len() as u64;
    connect_ingest(&state, &principal, &headers, bytes, || decode_otlp(&req.0)).await?;
    Ok(ConnectResponse::new(
        pb::otlp_profiles::ExportProfilesServiceResponse {
            partial_success: None,
        },
    ))
}
