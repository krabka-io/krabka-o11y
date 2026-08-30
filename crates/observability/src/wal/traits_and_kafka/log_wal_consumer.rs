use super::{KafkaWalRecord, Time, WalConsumerError, WalPosition, async_trait};

#[async_trait]
pub trait LogWalConsumer: Send + 'static {
    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError>;

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError>;
}
