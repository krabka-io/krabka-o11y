use super::{Bytes, ProduceError, WalRecord};

/// Testable sink for metrics WAL records.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError>;
}
