use std::future::Future;

use super::{
    Acks, Arc, ClientResourcePolicy, LogWalSink, Producer, ProducerError, WalLogRecord,
    WalSinkError, async_trait, build_kafka_wal_record,
};
use crate::wal_produce::{ProduceWindow, WalBatchError, write_batch_pipelined};

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

    /// Hands one record to the producer and returns the future for its ack.
    ///
    /// The two halves are separate because only the first decides order.
    /// `Producer::send` appends the record to the partition accumulator, and
    /// the returned future resolves when the broker acks it.
    async fn enqueue(
        &self,
        record: WalLogRecord,
    ) -> Result<impl Future<Output = Result<(), WalSinkError>> + use<>, WalSinkError> {
        let delivery = self
            .producer
            .send(build_kafka_wal_record(&self.topic, &record)?)
            .await;
        Ok(async move {
            delivery
                .await
                .map_err(|_| WalSinkError::DeliveryCanceled)??;
            Ok(())
        })
    }
}

#[async_trait]
impl LogWalSink for KafkaLogWalSink {
    #[cfg_attr(test, mutants::skip)]
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        self.enqueue(record).await?.await
    }

    #[cfg_attr(test, mutants::skip)]
    async fn append_batch(
        &self,
        records: Vec<WalLogRecord>,
    ) -> Result<(), WalBatchError<WalSinkError>> {
        write_batch_pipelined(records, ProduceWindow::default(), |record| {
            self.enqueue(record)
        })
        .await
    }
}
