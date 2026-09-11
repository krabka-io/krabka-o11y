/// Errors across the traces ingest and query pipeline.
#[derive(Debug, thiserror::Error)]
pub enum TracesError {
    #[error("unsupported content-type: {0}")]
    UnsupportedContentType(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("limit exceeded: {0}")]
    Limit(String),
    #[error("rate limit exceeded: {0}")]
    RateLimit(String),
    #[error("payload exceeds limit {limit} bytes")]
    TooLarge { limit: usize },
    #[error("wal codec: {0}")]
    Wal(String),
    #[error("produce failed: {0}")]
    Produce(String),
    #[error("block build failed: {0}")]
    Block(String),
    /// A WAL batch that appended in part or not at all.
    ///
    /// This is separate from [`Self::Produce`] because it carries how much of
    /// the batch reached the broker. The status stays 500, so an OTLP exporter
    /// retries the whole export, and the traces read path drops the duplicate
    /// spans that the retry writes.
    #[error("wal append wrote {appended} of {total} records: {message}")]
    ProduceBatch {
        appended: usize,
        total: usize,
        message: String,
    },
}

impl From<krabka_observability::wal_produce::WalBatchError<TracesError>> for TracesError {
    fn from(error: krabka_observability::wal_produce::WalBatchError<TracesError>) -> Self {
        let (appended, total) = (error.appended(), error.total());
        Self::ProduceBatch {
            appended,
            total,
            message: error.into_source().to_string(),
        }
    }
}

impl TracesError {
    /// Map to the ingest-edge HTTP status that Tempo-shaped push endpoints
    /// use.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedContentType(_) => 415,
            Self::Decode(_) | Self::Invalid(_) | Self::Limit(_) | Self::TooLarge { .. } => 400,
            Self::RateLimit(_) => 429,
            Self::Wal(_) | Self::Produce(_) | Self::ProduceBatch { .. } | Self::Block(_) => 500,
        }
    }
}
