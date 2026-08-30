/// Errors that the `TraceQL` engine raises.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TraceqlError {
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
}

impl From<datafusion::error::DataFusionError> for TraceqlError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Exec(e.to_string())
    }
}
