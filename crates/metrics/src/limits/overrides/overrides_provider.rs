use super::*;

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
}
