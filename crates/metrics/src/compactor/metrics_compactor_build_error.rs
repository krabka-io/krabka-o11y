use super::*;

/// Errors raised while constructing live compactor role dependencies.
#[derive(Debug, thiserror::Error)]
pub enum MetricsCompactorBuildError {
    #[error(transparent)]
    Config(#[from] MetricsCompactorConfigError),

    #[error("metrics compactor consumer build failed: {0}")]
    Consumer(String),
}
