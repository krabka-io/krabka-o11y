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

    /// Whether any tenant's blocks can ever expire.
    ///
    /// False when every window is zero, including the default one an unlisted
    /// tenant reads. A deployment like that has nothing for a retention sweep
    /// to find, so the compactor can skip the pass over the index.
    #[must_use]
    pub fn expires_any_blocks(&self) -> bool {
        [&self.defaults]
            .into_iter()
            .chain(self.per_tenant.values())
            .any(|limits| limits.block_retention > Time::ZERO)
    }
}

impl RetentionWindows for OverridesProvider {
    // A tenant with no entry of its own answers from the defaults, so every
    // tenant has a window here and not only the listed ones.
    fn block_retention(&self, tenant: &str) -> Time {
        self.for_tenant(tenant).block_retention
    }
}
