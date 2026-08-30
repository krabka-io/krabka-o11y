use super::{SpanRecord, TracesError};

/// Append one already-encoded logical span record to the traces WAL.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError>;
}
