use super::*;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OverridesError {
    #[error("failed to parse runtime overrides YAML: {0}")]
    Yaml(String),
}
