use super::*;

pub(crate) fn admin_connection_options(
    client_resource_policy: ClientResourcePolicy,
) -> krabka_client_core::ConnectionOptions {
    krabka_client_core::ConnectionOptions {
        dispatch_queue_capacity: client_resource_policy.dispatch_queue_capacity,
        frame_max: client_resource_policy.frame_max,
        ..krabka_client_core::ConnectionOptions::default()
    }
}
