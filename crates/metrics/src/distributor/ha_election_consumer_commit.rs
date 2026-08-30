use super::*;

#[async_trait::async_trait]
pub trait HaElectionConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError>;
}
