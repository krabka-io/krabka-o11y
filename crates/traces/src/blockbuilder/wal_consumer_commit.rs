use super::TracesError;

/// Minimal WAL-consumer commit surface the block-builder loop drives.
///
/// This trait stays separate from [`WalConsumerPoll`], so a test can express
/// the commit-only invariant as its own recorded call. That invariant is that a
/// commit happens strictly after a durable flush.
#[async_trait::async_trait]
pub trait WalConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), TracesError>;
}
