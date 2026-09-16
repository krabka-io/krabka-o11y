use super::{KafkaWalRecord, Time, WalConsumerError, WalPosition, async_trait};
use crate::ReadinessGate;

#[async_trait]
pub trait LogWalConsumer: Send + 'static {
    /// Connects readiness to consumers that can report broker catch-up.
    fn set_catch_up_gate(&mut self, gate: ReadinessGate) {
        gate.mark_ready();
    }

    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError>;

    /// Reports that the last polled batch was applied successfully.
    async fn records_applied(&mut self) {}

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError>;
}
