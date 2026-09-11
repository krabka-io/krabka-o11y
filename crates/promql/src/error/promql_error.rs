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

    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A per-tenant query limit rejected the evaluation.
    ///
    /// The message is the limit error's own message, so the HTTP layer reports
    /// the same text for an engine-raised limit and for one raised at the API
    /// boundary.
    #[error("{0}")]
    Limit(#[from] krabka_metrics::LimitError),
}

impl From<datafusion::error::DataFusionError> for PromqlError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::Exec(error.to_string())
    }
}
