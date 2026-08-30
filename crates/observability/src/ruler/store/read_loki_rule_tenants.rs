use super::*;

pub(crate) fn read_loki_rule_tenants(path: &FsPath) -> Result<LokiRuleTenants, LokiRuleStoreError> {
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
