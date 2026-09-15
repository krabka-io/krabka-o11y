use krabka_observability::{
    ReadinessGate, wal_consumer_metrics::WalConsumerMetrics,
    wal_group_assignment::WalAssignmentWatch,
};

use super::{
    AsyncMutex, Consumer, SinkError, SpanRecord, SpanSource, Time, async_trait,
    decode_consumer_records, millis,
};

/// Kafka-backed source for the traces WAL consumer group.
pub struct KafkaSpanSource {
    pub(crate) consumer: AsyncMutex<Consumer>,
    pub(crate) poll_timeout: Time,
    metrics: WalConsumerMetrics,
    assignment: AsyncMutex<WalAssignmentWatch>,
}

impl KafkaSpanSource {
    #[must_use]
    pub fn new(consumer: Consumer, metrics: WalConsumerMetrics, catch_up: ReadinessGate) -> Self {
        Self {
            consumer: AsyncMutex::new(consumer),
            poll_timeout: millis(500),
            assignment: AsyncMutex::new(WalAssignmentWatch::with_catch_up(
                metrics.clone(),
                catch_up,
            )),
            metrics,
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
            .inspect_err(|_| self.metrics.record_poll_failure())
            .map_err(|err| SinkError::Source(err.to_string()))?;
        self.metrics.record_poll(&records);
        self.assignment
            .lock()
            .await
            .observe_consumer(&consumer, !records.is_empty())
            .await;
        decode_consumer_records(records)
    }

    async fn commit(&self) -> Result<(), SinkError> {
        let consumer = self.consumer.lock().await;
        consumer
            .commit_sync()
            .await
            .map_err(|err| SinkError::Source(err.to_string()))?;
        self.metrics.record_commit();
        self.assignment
            .lock()
            .await
            .observe_applied(&consumer)
            .await;
        Ok(())
    }
}
