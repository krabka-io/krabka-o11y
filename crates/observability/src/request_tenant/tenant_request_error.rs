use super::{Error, TenantResolveError};

/// Why a request's `X-Scope-OrgID` does not name the tenant a handler needs.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum TenantRequestError {
    /// The header is absent, or no part of it names a tenant, or a part is not
    /// a valid tenant id.
    #[error(transparent)]
    Resolve(#[from] TenantResolveError),

    /// The header names two or more distinct tenants, and the handler serves
    /// only one. The text is `dskit`'s `user.ErrTooManyOrgIDs`.
    #[error("multiple org IDs present")]
    MultipleOrgIds,
}
