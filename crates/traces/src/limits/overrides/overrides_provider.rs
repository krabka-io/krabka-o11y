use super::{
    HashMap, Limits, Map, OverridesError, PartialLimits, RetentionWindows, RuntimeFile, RwLock,
    Time, TimeExt, Value, merge_limits,
};

#[derive(Clone, Debug)]
pub struct OverridesProvider {
    pub(crate) defaults: Limits,
    pub(crate) per_tenant: HashMap<String, Limits>,
    api: std::sync::Arc<RwLock<HashMap<String, VersionedOverride>>>,
}

#[derive(Clone, Debug)]
struct VersionedOverride {
    raw: Value,
    limits: PartialLimits,
    version: u64,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum OverrideMutationError {
    #[error("overrides do not exist")]
    NotFound,
    #[error("version does not match")]
    VersionMismatch,
    #[error("invalid overrides: {0}")]
    Invalid(String),
}

impl OverridesProvider {
    #[must_use]
    pub fn new(defaults: Limits) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
            api: std::sync::Arc::new(RwLock::new(HashMap::new())),
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
            api: std::sync::Arc::new(RwLock::new(HashMap::new())),
        })
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> Limits {
        let base = *self.per_tenant.get(tenant).unwrap_or(&self.defaults);
        let api = self
            .api
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        api.get(tenant)
            .map_or(base, |entry| merge_limits(&base, &entry.limits))
    }

    #[must_use]
    pub fn api_get(&self, tenant: &str) -> Option<(Value, String)> {
        self.api
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(tenant)
            .map(|entry| (entry.raw.clone(), entry.version.to_string()))
    }

    /// Replace a tenant's API overrides when `expected` matches its version.
    ///
    /// # Errors
    /// Returns [`OverrideMutationError`] for invalid limits or a stale version.
    pub fn api_set(
        &self,
        tenant: &str,
        raw: Value,
        expected: &str,
    ) -> Result<String, OverrideMutationError> {
        let limits = parse_api_limits(&raw)?;
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = api.get(tenant).map_or(0, |entry| entry.version);
        if expected.parse::<u64>().ok() != Some(current) {
            return Err(OverrideMutationError::VersionMismatch);
        }
        let version = current.saturating_add(1);
        api.insert(
            tenant.to_string(),
            VersionedOverride {
                raw,
                limits,
                version,
            },
        );
        Ok(version.to_string())
    }

    /// Apply an RFC 7386 merge patch atomically.
    ///
    /// # Errors
    /// Returns [`OverrideMutationError`] when the merged limits are invalid.
    pub fn api_patch(
        &self,
        tenant: &str,
        patch: Value,
    ) -> Result<(Value, String), OverrideMutationError> {
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut raw = api
            .get(tenant)
            .map_or_else(|| Value::Object(Map::default()), |entry| entry.raw.clone());
        merge_json(&mut raw, patch);
        let limits = parse_api_limits(&raw)?;
        let version = api
            .get(tenant)
            .map_or(1, |entry| entry.version.saturating_add(1));
        api.insert(
            tenant.to_string(),
            VersionedOverride {
                raw: raw.clone(),
                limits,
                version,
            },
        );
        Ok((raw, version.to_string()))
    }

    /// Delete a tenant's API overrides when `expected` matches its version.
    ///
    /// # Errors
    /// Returns [`OverrideMutationError`] when the override is absent or stale.
    pub fn api_delete(&self, tenant: &str, expected: &str) -> Result<(), OverrideMutationError> {
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = api.get(tenant).ok_or(OverrideMutationError::NotFound)?;
        if expected.parse::<u64>().ok() != Some(entry.version) {
            return Err(OverrideMutationError::VersionMismatch);
        }
        api.remove(tenant);
        Ok(())
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

fn parse_api_limits(raw: &Value) -> Result<PartialLimits, OverrideMutationError> {
    serde_json::from_value(raw.clone())
        .map_err(|error| OverrideMutationError::Invalid(error.to_string()))
}

fn merge_json(target: &mut Value, patch: Value) {
    match patch {
        Value::Object(patch) => {
            if !target.is_object() {
                *target = Value::Object(Map::default());
            }
            let target = target.as_object_mut().expect("target was made an object");
            for (key, value) in patch {
                if value.is_null() {
                    target.remove(&key);
                } else {
                    merge_json(target.entry(key).or_insert(Value::Null), value);
                }
            }
        }
        value => *target = value,
    }
}
