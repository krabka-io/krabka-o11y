use super::*;

/// Kafka-backed source for the traces WAL consumer group.
pub struct KafkaSpanSource {
    pub(crate) consumer: AsyncMutex<Consumer>,
    pub(crate) poll_timeout: Time,
}

impl KafkaSpanSource {
    #[must_use]
    pub fn new(consumer: Consumer) -> Self {
        Self {
            consumer: AsyncMutex::new(consumer),
            poll_timeout: millis(500),
        }
    }

    #[must_use]
    pub fn with_poll_timeout(mut self, poll_timeout: Time) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }
}

#[async_trait]
impl SpanSource for KafkaSpanSource {
    async fn poll(&self, _max: usize) -> Result<Vec<SpanRecord>, SinkError> {
        let mut consumer = self.consumer.lock().await;
        let records = consumer
            .poll(self.poll_timeout)
            .await
            .map_err(|err| SinkError::Source(err.to_string()))?;
        decode_consumer_records(records)
    }

    async fn commit(&self) -> Result<(), SinkError> {
        self.consumer
            .lock()
            .await
            .commit_sync()
            .await
            .map_err(|err| SinkError::Source(err.to_string()))
    }
}
