use super::*;

#[derive(Debug, thiserror::Error)]
pub enum OverridesError {
    #[error("profiles overrides yaml: {0}")]
    Yaml(String),
    #[error("profiles overrides for tenant {tenant:?}: {reason}")]
    Invalid { tenant: String, reason: String },
}
