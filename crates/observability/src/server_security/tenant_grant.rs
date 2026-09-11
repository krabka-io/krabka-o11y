use std::{collections::BTreeSet, sync::Arc};

use krabka_blockstore::TenantId;

/// The tenants a principal may read and write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantGrant {
    /// Every tenant, written as `["*"]` in the credentials file.
    All,
    /// Only the listed tenants. An empty set allows no tenant.
    Only(Arc<BTreeSet<TenantId>>),
}

impl TenantGrant {
    /// Whether the grant includes `tenant`.
    #[must_use]
    pub fn allows(&self, tenant: &TenantId) -> bool {
        match self {
            Self::All => true,
            Self::Only(tenants) => tenants.contains(tenant),
        }
    }
}
