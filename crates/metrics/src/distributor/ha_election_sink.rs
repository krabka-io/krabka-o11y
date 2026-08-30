use super::*;

/// Testable sink for compacted HA election records.
#[async_trait::async_trait]
pub trait HaElectionSink: Send + Sync {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError>;
}
