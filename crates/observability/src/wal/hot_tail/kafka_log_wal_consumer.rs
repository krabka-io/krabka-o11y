use super::{
    AutoOffsetReset, ClientResourcePolicy, ClientSecurity, Consumer, ConsumerError, KafkaWalHeader,
    KafkaWalRecord, LogWalConsumer, Offset, PartitionIndex, Time, WalConsumerError,
    WalConsumerMetrics, WalPosition, async_trait,
};
use crate::wal_group_assignment::WalAssignmentWatch;

pub struct KafkaLogWalConsumer {
    pub(crate) consumer: Consumer,
    metrics: WalConsumerMetrics,
    assignment: WalAssignmentWatch,
}

impl KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    /// Connects in plain text with the default Kafka connection limits.
    ///
    /// # Errors
    /// Returns an error when the consumer cannot start.
    pub async fn connect(
        bootstrap: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ConsumerError> {
        Self::connect_with_client_resource_policy(
            bootstrap,
            group_id,
            topic,
            ClientResourcePolicy::default(),
            None,
        )
        .await
    }

    /// Connects with the supplied validated Kafka connection limits, under the
    /// WAL client `security` that the service loaded.
    ///
    /// `None` connects in plain text.
    ///
    /// # Errors
    /// Returns an error when the consumer cannot start.
    pub async fn connect_with_client_resource_policy(
        bootstrap: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        client_resource_policy: ClientResourcePolicy,
        security: Option<ClientSecurity>,
    ) -> Result<Self, ConsumerError> {
        let topic = topic.into();
        let consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-observability-block-builder")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .maybe_security(security)
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(vec![topic])
            .build()
            .await?;
        let metrics = WalConsumerMetrics::unregistered();
        Ok(Self {
            consumer,
            assignment: WalAssignmentWatch::new(metrics.clone()),
            metrics,
        })
    }

    /// The same consumer, with its polls counted in `metrics`.
    ///
    /// A consumer built without this records nothing, so the role that owns it
    /// must call this. Both the hot-tail poller and the compactor read through
    /// this one type, so one call covers the logs signal.
    #[must_use]
    pub fn with_metrics(mut self, metrics: WalConsumerMetrics) -> Self {
        // The assignment watch reports through the same bundle, so it is
        // rebuilt here rather than left pointing at the unregistered one.
        self.assignment = WalAssignmentWatch::new(metrics.clone());
        self.metrics = metrics;
        self
    }

    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn close(self) {
        let _ = self.consumer.close().await;
    }
}

#[async_trait]
impl LogWalConsumer for KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        let records = self
            .consumer
            .poll(timeout)
            .await
            .inspect_err(|_| self.metrics.record_poll_failure())?;
        // Recorded before the mapping below, so a poll that arrived is counted
        // even when a record in it turns out to carry no value.
        self.metrics.record_poll(&records);
        // Read after the poll, so the snapshot is the one the fetch was served
        // against. A revocation here says the group abandoned whatever the
        // compactor had buffered for the lost partitions. See
        // `krabka_observability::wal_group_assignment`.
        self.assignment.observe_consumer(&self.consumer).await;
        records
            .into_iter()
            .map(|record| {
                let value = record
                    .value
                    .ok_or_else(|| WalConsumerError::MissingValue {
                        topic: record.topic.clone(),
                        partition: record.partition,
                        offset: record.offset,
                    })?
                    .to_vec();
                Ok(KafkaWalRecord {
                    value,
                    partition: PartitionIndex(record.partition),
                    offset: Offset(record.offset),
                    timestamp_ms: Some(record.timestamp),
                    headers: record
                        .headers
                        .into_iter()
                        .map(|header| KafkaWalHeader {
                            key: header.key,
                            value: header.value.map(|value| value.to_vec()),
                        })
                        .collect(),
                })
            })
            .collect()
    }

    #[cfg_attr(test, mutants::skip)]
    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        self.consumer.commit_sync().await?;
        Ok(())
    }
}
