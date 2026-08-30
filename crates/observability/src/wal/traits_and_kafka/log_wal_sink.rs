use super::*;

#[async_trait]
pub trait LogWalSink: Send + Sync + 'static {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError>;
}
