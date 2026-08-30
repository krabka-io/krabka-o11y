use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TenantLimitConfig {
    #[serde(default)]
    pub default: TenantLimits,
    #[serde(default)]
    pub tenants: BTreeMap<String, TenantLimits>,
}

impl TenantLimitConfig {
    #[must_use]
    pub fn with_tenant_limits(mut self, tenant: impl Into<String>, limits: TenantLimits) -> Self {
        self.tenants.insert(tenant.into(), limits);
        self
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &TenantLimits {
        self.tenants.get(tenant).unwrap_or(&self.default)
    }
}
