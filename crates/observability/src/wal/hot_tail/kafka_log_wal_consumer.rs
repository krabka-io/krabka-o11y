use super::*;

pub struct KafkaLogWalConsumer {
    pub(crate) consumer: Consumer,
}

impl KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
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
        )
        .await
    }

    /// Connects with the supplied validated Kafka connection limits.
    ///
    /// # Errors
    /// Returns an error when the consumer cannot start.
    pub async fn connect_with_client_resource_policy(
        bootstrap: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ConsumerError> {
        let topic = topic.into();
        let consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-observability-compactor")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(vec![topic])
            .build()
            .await?;
        Ok(Self { consumer })
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
        self.consumer
            .poll(timeout)
            .await?
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
