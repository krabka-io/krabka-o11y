use super::Error;

/// Why a runtime overrides file did not load.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverridesError {
    #[error("failed to parse logs runtime overrides YAML: {0}")]
    Yaml(String),
    #[error("failed to read logs runtime overrides file {path}: {reason}")]
    Read { path: String, reason: String },
}
