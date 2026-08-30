use super::{BlockStoreError, DataFusionError, Error, SeriesFingerprint};

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("invalid query column `{column}`: expected {expected}")]
    InvalidColumn {
        column: &'static str,
        expected: &'static str,
    },
    #[error("invalid metric query step {0}")]
    InvalidStep(i64),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("metric query contains pipeline error `{error}`")]
    MetricPipelineError {
        error: String,
        details: Option<String>,
    },
    #[error(transparent)]
    StructuredMetadata(#[from] serde_json::Error),
}
