use super::*;

impl SharedLokiRules {
    pub(crate) fn from_data_root(root: impl AsRef<FsPath>) -> Result<Self, LokiRuleStoreError> {
        let path = loki_ruler_rules_path(root.as_ref());
        Ok(Self {
            tenants: Arc::new(Mutex::new(read_loki_rule_tenants(&path)?)),
            storage_path: Some(Arc::new(path)),
        })
    }

    pub(crate) fn persist_snapshot(
        &self,
        tenants: &LokiRuleTenants,
    ) -> Result<(), LokiRuleStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        write_loki_rule_tenants(path, tenants)
    }
}
