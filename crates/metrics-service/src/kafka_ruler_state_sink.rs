use super::*;

pub struct KafkaRulerStateSink {
    pub(crate) producer: Arc<Producer>,
    pub(crate) topic: String,
}

impl KafkaRulerStateSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }

    pub(crate) async fn append_state_record(
        &self,
        record: RulerStateWalRecord,
    ) -> Result<(), RulerWalError> {
        let key = ruler_state_compaction_key(&record);
        let value = record
            .encode()
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
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

#[async_trait::async_trait]
impl RulerStateSink for KafkaRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.append_state_record(RulerStateWalRecord::Group(record))
            .await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.append_state_record(RulerStateWalRecord::Alert(record))
            .await
    }
}
