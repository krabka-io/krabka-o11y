use std::future::Future;

use super::{
    Acks, Arc, ClientResourcePolicy, ClientSecurity, LogWalSink, Producer, ProducerError,
    WalLogRecord, WalSinkError, async_trait, build_kafka_wal_record,
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
    /// Connects in plain text with the default Kafka connection limits.
    ///
    /// # Errors
    /// Returns an error when the producer cannot start.
    pub async fn connect(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ProducerError> {
        Self::connect_with_client_resource_policy(
            bootstrap,
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
    /// Returns an error when the producer cannot start.
    pub async fn connect_with_client_resource_policy(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
        client_resource_policy: ClientResourcePolicy,
        security: Option<ClientSecurity>,
    ) -> Result<Self, ProducerError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-observability-distributor")
            // Log pushes enqueue many records before awaiting their acks.
            // Keep the default batch size for custom frame limits.
            .batch_size(
                if client_resource_policy.frame_max.size()
                    == krabka_client_core::DEFAULT_CLIENT_FRAME_MAX
                {
                    64 * 1024
                } else {
                    krabka_client_producer::DEFAULT_PRODUCER_BATCH_BYTES
                },
            )
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .maybe_security(security)
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self::new(producer, topic))
    }

    /// Hands one record to the producer and returns the future for its ack.
    ///
    /// The two halves are separate because only the first decides order.
    /// `Producer::enqueue` appends the record to the partition accumulator, and
    /// the returned future resolves when the broker acks it.
    async fn enqueue(
        &self,
        record: WalLogRecord,
    ) -> Result<impl Future<Output = Result<(), WalSinkError>> + use<>, WalSinkError> {
        let delivery = self
            .producer
            .enqueue(build_kafka_wal_record(&self.topic, &record)?)
            .await?;
        Ok(async move {
            delivery.await?;
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
