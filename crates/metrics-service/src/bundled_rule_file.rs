/// One Prometheus rule file, as the ruler reads it from disk.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct BundledRuleFile {
    pub(crate) groups: Vec<serde_yaml::Value>,
}
