use krabka_blockstore::RetentionWindows;

use super::{HashMap, Limits, OverridesError, RuntimeFile, TenantId, Time, TimeExt, merge_limits};

/// The one place a tenant's [`Limits`] come from.
///
/// Each logs service builds exactly one provider and shares it with the
/// distributor and the querier, so both gates answer a tenant with the same
/// numbers.
#[derive(Clone, Debug)]
pub struct OverridesProvider {
    defaults: Limits,
    per_tenant: HashMap<String, Limits>,
}

impl OverridesProvider {
    /// A provider with no per-tenant entry: every tenant gets `defaults`.
    #[must_use]
    pub fn new(defaults: Limits) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
        }
    }

    /// Parses a runtime overrides file over `Limits::default()`.
    ///
    /// # Errors
    ///
    /// [`OverridesError::Yaml`] when the text is not a runtime overrides file,
    /// when a key is misspelled, or when a limit is negative.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        Self::from_yaml_over(yaml, &Limits::default())
    }

    /// Parses a runtime overrides file over the process defaults the scalar CLI
    /// flags built.
    ///
    /// The file's `defaults` block merges over `base`, and each tenant's entry
    /// merges over the result. An operator who sets both a flag and a
    /// `defaults` key therefore gets the file's value, and the file is the one
    /// they can change without a restart.
    ///
    /// # Errors
    ///
    /// [`OverridesError::Yaml`] when the text is not a runtime overrides file,
    /// when a key is misspelled, or when a limit is negative.
    pub fn from_yaml_over(yaml: &str, base: &Limits) -> Result<Self, OverridesError> {
        let runtime: RuntimeFile =
            serde_yaml::from_str(yaml).map_err(|error| OverridesError::Yaml(error.to_string()))?;
        let defaults = merge_limits(base, &runtime.defaults);
        let per_tenant = runtime
            .overrides
            .into_iter()
            .map(|(tenant, partial)| (tenant, merge_limits(&defaults, &partial)))
            .collect();
        Ok(Self {
            defaults,
            per_tenant,
        })
    }

    /// The limits that apply to `tenant`.
    #[must_use]
    pub fn for_tenant(&self, tenant: &TenantId) -> &Limits {
        self.per_tenant
            .get(tenant.as_str())
            .unwrap_or(&self.defaults)
    }

    /// The limits that apply to a tenant with no entry of its own.
    #[must_use]
    pub fn defaults(&self) -> &Limits {
        &self.defaults
    }

    /// Whether `tenant` has an entry of its own in the overrides file.
    #[must_use]
    pub fn has_tenant_override(&self, tenant: &TenantId) -> bool {
        self.per_tenant.contains_key(tenant.as_str())
    }

    /// Whether any tenant's blocks can ever expire.
    ///
    /// False when every window is zero, including the default one that an
    /// unlisted tenant reads. A deployment like that has nothing for the
    /// retention sweep to delete, so the compactor should not pay for a pass
    /// over the object store on every loop.
    #[must_use]
    pub fn expires_any_blocks(&self) -> bool {
        [&self.defaults]
            .into_iter()
            .chain(self.per_tenant.values())
            .any(|limits| limits.retention_period > Time::ZERO)
    }
}

impl RetentionWindows for OverridesProvider {
    // A tenant with no entry of its own answers from the defaults, so every
    // tenant has a window here and not only the listed ones. The lookup takes
    // a `&str` rather than a `TenantId`, because the sweep reads its tenants
    // back out of object-store paths and not out of a request header.
    fn block_retention(&self, tenant: &str) -> Time {
        self.per_tenant
            .get(tenant)
            .unwrap_or(&self.defaults)
            .retention_period
    }
}
