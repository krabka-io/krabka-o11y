use std::sync::Arc;

use krabka_blockstore::TenantId;

use super::{AuthMethod, SecurityEventSink, TenantGrant};

/// Who sent a request, as the authentication layer decided it.
///
/// The authentication layer puts one into the extensions of every request it
/// passes on, except on an [`UnauthenticatedRoutes`](super::UnauthenticatedRoutes)
/// route. A handler that extracts it on such a route gets a 500, so a route
/// added to that list by mistake fails closed. Every field is shared, so a
/// clone costs a few reference-count updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// The server has no credentials file, which is the upstream default.
    ///
    /// [`authorize_tenant`](super::authorize_tenant) and
    /// [`authorize_admin`](super::authorize_admin) allow everything.
    Unauthenticated,
    /// A credential matched a principal in the credentials file.
    Authenticated {
        /// The principal's `name` in the credentials file.
        name: Arc<str>,
        /// The credential that matched.
        method: AuthMethod,
        /// The tenants the principal may use.
        tenants: TenantGrant,
        /// Whether the principal may call admin operations.
        admin: bool,
        /// Where the authorization helpers report a denial.
        events: SecurityEventSink,
    },
}

impl Principal {
    /// The Kafka ACL principal to check for a request on `tenant`.
    ///
    /// An authenticated request checks `User:{name}`. An unauthenticated
    /// request checks `User:{tenant}`, which keeps the ACLs of a server
    /// without a credentials file as they are.
    #[must_use]
    pub fn acl_principal(&self, tenant: &TenantId) -> String {
        match self {
            Self::Unauthenticated => format!("User:{tenant}"),
            Self::Authenticated { name, .. } => format!("User:{name}"),
        }
    }

    /// The principal's name, or `None` when the request is unauthenticated.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Unauthenticated => None,
            Self::Authenticated { name, .. } => Some(name),
        }
    }
}
