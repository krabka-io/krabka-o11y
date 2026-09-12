use krabka_blockstore::RetentionWindows;
use krabka_units::Time;

use super::{HashMap, Limits, OverridesError, RuntimeFile, TenantId};

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
        Self::from_yaml_with_defaults(yaml, &Limits::default())
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_yaml_with_defaults(yaml: &str, defaults: &Limits) -> Result<Self, OverridesError> {
        let parsed: RuntimeFile =
            serde_yaml::from_str(yaml).map_err(|err| OverridesError::Yaml(err.to_string()))?;
        parsed
            .defaults
            .validate()
            .map_err(|reason| OverridesError::InvalidDefaults { reason })?;
        // The file's `defaults` block lands first, so a tenant entry departs
        // from what the operator wrote rather than from the compiled-in value.
        let defaults = parsed.defaults.merge_over(defaults);
        let mut per_tenant = HashMap::new();
        for (tenant, partial) in parsed.overrides {
            if let Err(reason) = partial.validate() {
                return Err(OverridesError::Invalid { tenant, reason });
            }
            per_tenant.insert(tenant, partial.merge_over(&defaults));
        }
        Ok(Self {
            defaults,
            per_tenant,
        })
    }

    /// The limits of `tenant`: its own entry, or the defaults when the file
    /// lists no entry for it.
    #[must_use]
    pub fn for_tenant(&self, tenant: &TenantId) -> &Limits {
        self.per_tenant
            .get(tenant.as_str())
            .unwrap_or(&self.defaults)
    }
}

impl RetentionWindows for OverridesProvider {
    // Keyed by the raw string rather than by a parsed `TenantId`: the compactor
    // reads the tenant out of a block record the ingest path already validated,
    // and a tenant that failed to parse here would silently keep its blocks
    // forever.
    fn block_retention(&self, tenant: &str) -> Time {
        self.per_tenant
            .get(tenant)
            .unwrap_or(&self.defaults)
            .compactor_blocks_retention_period
    }
}
