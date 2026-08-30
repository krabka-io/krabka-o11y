use super::{Arc, HaElectionRecord, HaElectionSink, ProduceError, Producer, ha_election_compaction_key, keyed_producer_record};

/// Producer-backed compacted HA election sink.
pub struct KafkaHaElectionSink {
    pub(crate) producer: Arc<Producer>,
    pub(crate) topic: String,
}

impl KafkaHaElectionSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }
}

#[async_trait::async_trait]
impl HaElectionSink for KafkaHaElectionSink {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError> {
        let key = ha_election_compaction_key(&record);
        let value = record
            .encode()
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        let ack = self
            .producer
            .send(keyed_producer_record(self.topic.clone(), key, value))
            .await;
        ack.await
            .map_err(|error| ProduceError::Append(error.to_string()))?
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        Ok(())
    }
}
