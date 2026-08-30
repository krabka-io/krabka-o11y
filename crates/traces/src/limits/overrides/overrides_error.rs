use super::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OverridesError {
    #[error("failed to parse overrides yaml: {0}")]
    Yaml(String),
}
