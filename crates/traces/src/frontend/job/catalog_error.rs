use super::*;

/// Errors enumerating blocks.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("block catalog error: {0}")]
    Backend(String),
}
