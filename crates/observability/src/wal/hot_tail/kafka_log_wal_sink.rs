use super::*;

#[derive(Clone)]
pub struct KafkaLogWalSink {
    pub(crate) producer: Arc<Producer>,
    pub(crate) topic: String,
}

impl KafkaLogWalSink {
    #[must_use]
    pub fn new(producer: Producer, topic: impl Into<String>) -> Self {
        Self {
            producer: Arc::new(producer),
            topic: topic.into(),
        }
    }

    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub async fn connect(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ProducerError> {
        Self::connect_with_client_resource_policy(bootstrap, topic, ClientResourcePolicy::default())
            .await
    }

    /// Connects with the supplied validated Kafka connection limits.
    ///
    /// # Errors
    /// Returns an error when the producer cannot start.
    pub async fn connect_with_client_resource_policy(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ProducerError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-observability-distributor")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self::new(producer, topic))
    }
}

#[async_trait]
impl LogWalSink for KafkaLogWalSink {
    #[cfg_attr(test, mutants::skip)]
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        let delivery = self
            .producer
            .send(build_kafka_wal_record(&self.topic, &record)?)
            .await;
        delivery
            .await
            .map_err(|_| WalSinkError::DeliveryCanceled)??;
        Ok(())
    }
}
