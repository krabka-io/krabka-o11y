use super::*;

pub(crate) fn write_loki_rule_tenants(
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
