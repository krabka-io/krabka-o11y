use super::*;

pub struct KafkaRecordingRuleWalSink {
    pub(crate) producer: Arc<Producer>,
    pub(crate) topic: String,
}

impl KafkaRecordingRuleWalSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }
}

#[async_trait::async_trait]
impl RecordingRuleWalSink for KafkaRecordingRuleWalSink {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError> {
        let value = record
            .encode()
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        let key = partition_key(&record.tenant, record.series_fingerprint());
        let ack = self
            .producer
            .send(keyed_producer_record(self.topic.clone(), key, value))
            .await;
        ack.await
            .map_err(|error| RulerWalError::Append(error.to_string()))?
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        Ok(())
    }
}
