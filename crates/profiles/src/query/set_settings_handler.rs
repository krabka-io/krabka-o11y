use super::{ConnectError, ConnectRequest, ConnectResponse, pb};

/// Pyroscope `settings.v1.SettingsService/Set`.
///
/// Krabka does not persist settings. This handler echoes the value back so the
/// optimistic UI update of the app succeeds for the session.
pub(crate) async fn set_settings_handler(
    req: ConnectRequest<pb::settings::v1::SetSettingsRequest>,
) -> Result<ConnectResponse<pb::settings::v1::SetSettingsResponse>, ConnectError> {
    Ok(ConnectResponse::new(
        pb::settings::v1::SetSettingsResponse {
            setting: req.0.setting,
        },
    ))
}
