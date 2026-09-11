use super::*;

/// Builds a tenant id for a test from a name the test knows is valid.
pub(crate) fn tenant_id(name: &str) -> TenantId {
    TenantId::new(name).expect("a test tenant name is a valid tenant id")
}
