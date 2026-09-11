use super::{Error, TenantIdError};

/// Why a request does not resolve to a tenant.
///
/// The variants are separate because upstream reports them differently. A
/// missing tenant is Grafana's `no org id` error, and a malformed one names the
/// rule the id broke.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TenantResolveError {
    /// The request names no tenant, and the policy is [`super::TenantPolicy::Required`].
    #[error("no org id")]
    Missing,

    /// The request names a tenant that is not a valid tenant id.
    #[error(transparent)]
    Invalid(#[from] TenantIdError),
}
