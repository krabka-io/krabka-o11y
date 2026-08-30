use super::{async_trait, SpanRecord, SinkError};

/// Input edge for decoded traces WAL records.
#[async_trait]
pub trait SpanSource: Send + Sync {
    async fn poll(&self, max: usize) -> Result<Vec<SpanRecord>, SinkError>;
    async fn commit(&self) -> Result<(), SinkError>;
}
