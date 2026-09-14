use super::{
    HashMap, Limits, Map, OverridesError, PartialLimits, RetentionWindows, RuntimeFile, RwLock,
    Time, TimeExt, Value, merge_limits,
};

#[derive(Clone, Debug)]
pub struct OverridesProvider {
    pub(crate) defaults: Limits,
    pub(crate) per_tenant: HashMap<String, Limits>,
    file_overrides: HashMap<String, PartialLimits>,
    api: std::sync::Arc<RwLock<HashMap<String, VersionedOverride>>>,
    api_path: Option<std::sync::Arc<std::path::PathBuf>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
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
    #[error("override storage failed: {0}")]
    Storage(String),
}

impl OverridesProvider {
    #[must_use]
    pub fn new(defaults: Limits) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
            file_overrides: HashMap::new(),
            api: std::sync::Arc::new(RwLock::new(HashMap::new())),
            api_path: None,
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
            .iter()
            .map(|(tenant, limits)| (tenant, merge_limits(&defaults, limits)))
            .map(|(tenant, limits)| (tenant.clone(), limits))
            .collect();

        Ok(Self {
            defaults,
            per_tenant,
            file_overrides: file.overrides,
            api: std::sync::Arc::new(RwLock::new(HashMap::new())),
            api_path: None,
        })
    }

    /// Attach a process-shared or filesystem-shared durable API override file.
    ///
    /// # Errors
    /// Returns an error when an existing state file cannot be read or decoded.
    pub fn with_api_file(
        mut self,
        path: std::path::PathBuf,
    ) -> Result<Self, OverrideMutationError> {
        self.api_path = Some(std::sync::Arc::new(path));
        self.refresh_api()?;
        Ok(self)
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> Limits {
        let _ = self.refresh_api();
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
        let _ = self.refresh_api();
        self.api
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(tenant)
            .map(|entry| (entry.raw.clone(), entry.version.to_string()))
    }

    #[must_use]
    pub fn api_conflicts_with_file(&self, tenant: &str, raw: &Value) -> bool {
        let Some(file) = self.file_overrides.get(tenant) else {
            return false;
        };
        let Ok(Value::Object(file)) = serde_json::to_value(file) else {
            return false;
        };
        raw.as_object().is_some_and(|raw| {
            raw.iter().any(|(key, value)| {
                !value.is_null()
                    && file
                        .get(key)
                        .is_some_and(|file_value| !file_value.is_null() && file_value != value)
            })
        })
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
        let _file_lock = self.lock_api_file()?;
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reload_locked(&mut api)?;
        let current = api.get(tenant).map_or(0, |entry| entry.version);
        if expected.parse::<u64>().ok() != Some(current) {
            return Err(OverrideMutationError::VersionMismatch);
        }
        let previous = api.clone();
        let version = current.saturating_add(1);
        api.insert(
            tenant.to_string(),
            VersionedOverride {
                raw,
                limits,
                version,
            },
        );
        if let Err(error) = self.persist_locked(&api) {
            *api = previous;
            return Err(error);
        }
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
        let _file_lock = self.lock_api_file()?;
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reload_locked(&mut api)?;
        let previous = api.clone();
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
        if let Err(error) = self.persist_locked(&api) {
            *api = previous;
            return Err(error);
        }
        Ok((raw, version.to_string()))
    }

    /// Delete a tenant's API overrides when `expected` matches its version.
    ///
    /// # Errors
    /// Returns [`OverrideMutationError`] when the override is absent or stale.
    pub fn api_delete(&self, tenant: &str, expected: &str) -> Result<(), OverrideMutationError> {
        let _file_lock = self.lock_api_file()?;
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reload_locked(&mut api)?;
        let entry = api.get(tenant).ok_or(OverrideMutationError::NotFound)?;
        if expected.parse::<u64>().ok() != Some(entry.version) {
            return Err(OverrideMutationError::VersionMismatch);
        }
        let previous = api.clone();
        api.remove(tenant);
        if let Err(error) = self.persist_locked(&api) {
            *api = previous;
            return Err(error);
        }
        Ok(())
    }

    fn refresh_api(&self) -> Result<(), OverrideMutationError> {
        let _file_lock = self.lock_api_file()?;
        let mut api = self
            .api
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reload_locked(&mut api)
    }

    fn lock_api_file(&self) -> Result<Option<std::fs::File>, OverrideMutationError> {
        let Some(path) = &self.api_path else {
            return Ok(None);
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| storage_error(&error))?;
        }
        let lock_path = path.with_extension("lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|error| storage_error(&error))?;
        file.lock().map_err(|error| storage_error(&error))?;
        Ok(Some(file))
    }

    fn reload_locked(
        &self,
        api: &mut HashMap<String, VersionedOverride>,
    ) -> Result<(), OverrideMutationError> {
        let Some(path) = &self.api_path else {
            return Ok(());
        };
        let bytes = match std::fs::read(path.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                api.clear();
                return Ok(());
            }
            Err(error) => return Err(storage_error(&error)),
        };
        *api = serde_json::from_slice(&bytes)
            .map_err(|error| OverrideMutationError::Storage(error.to_string()))?;
        Ok(())
    }

    fn persist_locked(
        &self,
        api: &HashMap<String, VersionedOverride>,
    ) -> Result<(), OverrideMutationError> {
        let Some(path) = &self.api_path else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(api)
            .map_err(|error| OverrideMutationError::Storage(error.to_string()))?;
        let temporary = path.with_extension("tmp");
        std::fs::write(&temporary, bytes).map_err(|error| storage_error(&error))?;
        std::fs::rename(temporary, path.as_ref()).map_err(|error| storage_error(&error))
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

fn storage_error(error: &impl ToString) -> OverrideMutationError {
    OverrideMutationError::Storage(error.to_string())
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
