//! The crate's error type.

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let pe: PromqlError = dfe.into();
        assert2::assert!(matches!(pe, PromqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = PromqlError::Unsupported("histogram_quantile".into());
        assert2::assert!(format!("{e}").contains("unsupported"));
    }
}

/// Errors the `PromQL` engine raises.
#[derive(Debug, thiserror::Error)]
pub enum PromqlError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("plan error: {0}")]
    Plan(String),
    #[error("execution error: {0}")]
    Exec(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("query frontend overloaded; retry after {retry_after_seconds}s")]
    Overloaded { retry_after_seconds: u64 },
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// A per-tenant query limit rejected the evaluation.
    #[error("{0}")]
    Limit(#[from] krabka_metrics::LimitError),
}

impl From<datafusion::error::DataFusionError> for PromqlError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::Exec(error.to_string())
    }
}

/// Internal convenience alias.
pub type Result<T> = std::result::Result<T, PromqlError>;
