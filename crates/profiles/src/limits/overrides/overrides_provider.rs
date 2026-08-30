use super::{HashMap, Limits, OverridesError, RuntimeFile};

/// Pyroscope-style runtime overrides resolved into full per-tenant limits.
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

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        Self::from_yaml_with_defaults(yaml, Limits::default())
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_yaml_with_defaults(yaml: &str, defaults: Limits) -> Result<Self, OverridesError> {
        let parsed: RuntimeFile =
            serde_yaml::from_str(yaml).map_err(|err| OverridesError::Yaml(err.to_string()))?;
        let mut per_tenant = HashMap::new();
        for (tenant, partial) in parsed.overrides {
            partial.validate(&tenant)?;
            per_tenant.insert(tenant, partial.merge_over(&defaults));
        }
        Ok(Self {
            defaults,
            per_tenant,
        })
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &Limits {
        self.per_tenant.get(tenant).unwrap_or(&self.defaults)
    }

    #[must_use]
    pub fn has_tenant_override(&self, tenant: &str) -> bool {
        self.per_tenant.contains_key(tenant)
    }
}
