use super::{MECHANISM_BASIC, MECHANISM_BEARER, MECHANISM_MTLS};
use crate::server_security::AuthMethod;

/// The audit mechanism string for the credential kind that authenticated a request.
#[must_use]
pub const fn mechanism_of(method: AuthMethod) -> &'static str {
    match method {
        AuthMethod::Bearer => MECHANISM_BEARER,
        AuthMethod::Basic => MECHANISM_BASIC,
        AuthMethod::ClientCertificate => MECHANISM_MTLS,
    }
}
