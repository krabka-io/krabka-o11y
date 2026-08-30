
/// Errors raised by the native-histogram Arrow codec.
#[derive(Debug, thiserror::Error)]
pub enum HistogramCodecError {
    #[error("span/count mismatch: spans claim {spans} buckets, got {counts} counts")]
    SpanCountMismatch { spans: usize, counts: usize },

    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}
