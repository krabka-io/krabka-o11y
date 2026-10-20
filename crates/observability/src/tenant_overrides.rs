use std::collections::HashMap;

/// Resolved defaults and per-tenant values for a tenant-keyed configuration.
#[derive(Clone, Debug)]
pub struct TenantOverrides<T> {
    defaults: T,
    per_tenant: HashMap<String, T>,
}

impl<T> TenantOverrides<T> {
    #[must_use]
    pub fn new(defaults: T) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
        }
    }

    #[must_use]
    pub fn from_resolved(defaults: T, per_tenant: HashMap<String, T>) -> Self {
        Self {
            defaults,
            per_tenant,
        }
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &T {
        self.per_tenant.get(tenant).unwrap_or(&self.defaults)
    }

    #[must_use]
    pub fn defaults(&self) -> &T {
        &self.defaults
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &T)> {
        self.per_tenant
            .iter()
            .map(|(tenant, value)| (tenant.as_str(), value))
    }

    pub fn values(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.defaults).chain(self.per_tenant.values())
    }
}
