/// No credentials. The request did not authenticate.
pub const MECHANISM_NONE: &str = "none";

/// HTTP basic authentication.
pub const MECHANISM_BASIC: &str = "basic";

/// An HTTP bearer token.
pub const MECHANISM_BEARER: &str = "bearer";

/// A TLS client certificate.
pub const MECHANISM_MTLS: &str = "mtls";

/// Every authentication-mechanism string that an audit event can carry.
///
/// The same strings fill [`AuditPrincipal::auth_method`](super::AuditPrincipal)
/// and the `mechanism` of an authentication event.
pub const MECHANISMS: [&str; 4] = [
    MECHANISM_NONE,
    MECHANISM_BASIC,
    MECHANISM_BEARER,
    MECHANISM_MTLS,
];
