use super::*;

pub(crate) fn ingest_limits_for_tenant(state: &DistributorState, tenant: &str) -> crate::ingest::TenantLimits {
    let base = state.limits.for_tenant(tenant);
    if !state.profile_overrides.has_tenant_override(tenant) {
        return base.clone();
    }
    let overrides = state.profile_overrides.for_tenant(tenant);
    merge_ingest_limits(base, overrides)
}
