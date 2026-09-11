use super::{ClientResourcePolicy, ClientSecurity, with_client_security};

/// The connection options of the admin client that the query authorizer and
/// the ingest limiter use.
///
/// `security` is the WAL client security that the service loaded. `None`
/// connects in plain text.
pub(crate) fn admin_connection_options(
    client_resource_policy: ClientResourcePolicy,
    security: Option<&ClientSecurity>,
) -> krabka_client_core::ConnectionOptions {
    with_client_security(
        krabka_client_core::ConnectionOptions {
            dispatch_queue_capacity: client_resource_policy.dispatch_queue_capacity,
            frame_max: client_resource_policy.frame_max,
            ..krabka_client_core::ConnectionOptions::default()
        },
        security,
    )
}
