use super::WalHeadConsumerError;

#[async_trait::async_trait]
pub trait WalHeadConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), WalHeadConsumerError>;
}
