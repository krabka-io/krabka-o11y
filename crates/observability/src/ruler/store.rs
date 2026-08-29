impl SharedLokiRules {
    fn from_data_root(root: impl AsRef<FsPath>) -> Result<Self, LokiRuleStoreError> {
        let path = loki_ruler_rules_path(root.as_ref());
        Ok(Self {
            tenants: Arc::new(Mutex::new(read_loki_rule_tenants(&path)?)),
            storage_path: Some(Arc::new(path)),
        })
    }

    fn persist_snapshot(&self, tenants: &LokiRuleTenants) -> Result<(), LokiRuleStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        write_loki_rule_tenants(path, tenants)
    }
}

fn loki_ruler_rules_path(root: &FsPath) -> PathBuf {
    root.join("loki-ruler-rules.json")
}

fn read_loki_rule_tenants(path: &FsPath) -> Result<LokiRuleTenants, LokiRuleStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == ErrorKind::NotFound => return Ok(LokiRuleTenants::new()),
        Err(source) => {
            return Err(LokiRuleStoreError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map_err(|source| LokiRuleStoreError::Json {
        path: path.to_path_buf(),
        source,
    })
}

fn write_loki_rule_tenants(
    path: &FsPath,
    tenants: &LokiRuleTenants,
) -> Result<(), LokiRuleStoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| LokiRuleStoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let tmp_path = path.with_file_name(".loki-ruler-rules.json.tmp");
    let payload =
        serde_json::to_vec_pretty(tenants).map_err(|source| LokiRuleStoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
    std::fs::write(&tmp_path, payload).map_err(|source| LokiRuleStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    std::fs::rename(&tmp_path, path).map_err(|source| LokiRuleStoreError::Io {
        path: path.to_path_buf(),
        source,
    })
}

