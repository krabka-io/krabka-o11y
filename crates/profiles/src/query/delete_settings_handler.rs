use super::{ConnectError, ConnectRequest, ConnectResponse, pb};

pub(crate) async fn delete_settings_handler(
    _req: ConnectRequest<pb::settings::v1::DeleteSettingsRequest>,
) -> Result<ConnectResponse<pb::settings::v1::DeleteSettingsResponse>, ConnectError> {
    Ok(ConnectResponse::new(
        pb::settings::v1::DeleteSettingsResponse {},
    ))
}
