use super::*;

/// Names the rule namespace of a bundled rule file from its file stem.
pub(crate) fn bundled_rules_namespace(path: &StdPath) -> Result<String, BundledRulesError> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
        .ok_or_else(|| BundledRulesError::NoNamespace {
            path: path.to_path_buf(),
        })
}
