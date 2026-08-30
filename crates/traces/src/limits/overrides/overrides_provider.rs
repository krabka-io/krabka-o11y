use super::{HashMap, Limits, OverridesError, RuntimeFile, merge_limits};

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
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        let defaults = Limits::default();
        let file = serde_yaml::from_str::<RuntimeFile>(yaml)
            .map_err(|err| OverridesError::Yaml(err.to_string()))?;
        let per_tenant = file
            .overrides
            .into_iter()
            .map(|(tenant, limits)| (tenant, merge_limits(&defaults, &limits)))
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
