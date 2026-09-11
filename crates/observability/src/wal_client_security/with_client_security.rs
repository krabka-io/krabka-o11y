use super::{ClientSecurity, ConnectionOptions};

/// Returns `options` with its security policy replaced by `security`.
///
/// `None` gives a plaintext connection, which is the default of
/// [`ConnectionOptions`]. Every other field keeps its value. Use this function
/// at a site that builds its own [`ConnectionOptions`]. A producer or
/// consumer builder takes the policy through `.maybe_security(..)` instead.
#[must_use]
pub fn with_client_security(
    options: ConnectionOptions,
    security: Option<&ClientSecurity>,
) -> ConnectionOptions {
    ConnectionOptions {
        security: security.cloned().map(Box::new),
        ..options
    }
}
