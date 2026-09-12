use super::{
    HashMap, Limits, OverridesError, RetentionWindows, RuntimeFile, Time, TimeExt, merge_limits,
};

#[derive(Clone, Debug)]
pub struct OverridesProvider {
    pub(crate) defaults: Limits,
    pub(crate) per_tenant: HashMap<String, Limits>,
}

impl OverridesProvider {
    #[must_use]
    pub fn new(defaults: Limits) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
        }
    }

    /// Parses Mimir-style `runtime.yaml` overrides.
    ///
    /// Tenant maps are partial by design. The `#[serde(default)]` below
    /// represents sparse per-tenant overrides, and not a
    /// backwards-compatibility migration.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        let runtime: RuntimeFile =
            serde_yaml::from_str(yaml).map_err(|error| OverridesError::Yaml(error.to_string()))?;
        let defaults = merge_limits(&Limits::default(), &runtime.defaults);
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

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &Limits {
        self.per_tenant.get(tenant).unwrap_or(&self.defaults)
    }

    /// Whether any tenant's blocks can ever expire.
    ///
    /// False when every window is zero, including the default one that an
    /// unlisted tenant reads. A deployment like that has nothing for a
    /// retention sweep to delete, so the operator should not pay for a pass
    /// over the bucket on every interval.
    #[must_use]
    pub fn expires_any_blocks(&self) -> bool {
        [&self.defaults]
            .into_iter()
            .chain(self.per_tenant.values())
            .any(|limits| limits.compactor_blocks_retention_period > Time::ZERO)
    }
}

impl RetentionWindows for OverridesProvider {
    // A tenant with no entry of its own answers from the defaults, so every
    // tenant has a window here and not only the listed ones.
    fn block_retention(&self, tenant: &str) -> Time {
        self.for_tenant(tenant).compactor_blocks_retention_period
    }
}
