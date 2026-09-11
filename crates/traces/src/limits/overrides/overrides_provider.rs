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

    /// Parse a runtime-overrides file over [`Limits::default`].
    ///
    /// # Errors
    /// Returns an error when the text is not the expected YAML document.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        Self::from_yaml_with_defaults(yaml, Limits::default())
    }

    /// Parse a runtime-overrides file over the limits this process was started
    /// with.
    ///
    /// The `defaults` are what an unlisted tenant gets, and they are also the
    /// base each listed tenant's entry merges over. A service builds them from
    /// its command line, so one flag moves every tenant that the file does not
    /// name.
    ///
    /// # Errors
    /// Returns an error when the text is not the expected YAML document.
    pub fn from_yaml_with_defaults(yaml: &str, defaults: Limits) -> Result<Self, OverridesError> {
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
