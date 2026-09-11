use std::sync::Arc;

use super::TenantGrant;

/// One principal from the credentials file, after validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredPrincipal {
    pub name: Arc<str>,
    pub tenants: TenantGrant,
    pub admin: bool,
}
