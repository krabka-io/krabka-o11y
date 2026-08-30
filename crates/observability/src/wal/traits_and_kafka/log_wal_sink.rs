use super::{WalLogRecord, WalSinkError, async_trait};

#[async_trait]
pub trait LogWalSink: Send + Sync + 'static {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError>;
}
