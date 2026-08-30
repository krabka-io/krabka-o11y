use super::*;

/// Pyroscope `settings.v1.SettingsService/Get`.
///
/// Krabka does not persist UI settings, so this handler reports an empty set.
/// The Grafana Profiles Drilldown app then uses its built-in defaults, the same
/// defaults as for a fresh Pyroscope tenant. Without this endpoint the init of
/// the app gets a 404 and the landing page renders empty.
pub(crate) async fn get_settings_handler(
    _req: ConnectRequest<pb::settings::v1::GetSettingsRequest>,
) -> Result<ConnectResponse<pb::settings::v1::GetSettingsResponse>, ConnectError> {
    Ok(ConnectResponse::new(
        pb::settings::v1::GetSettingsResponse {
            settings: Vec::new(),
        },
    ))
}
