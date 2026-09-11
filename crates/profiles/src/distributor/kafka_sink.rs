use std::future::Future;

use super::{
    Arc, Bytes, Header, PROFILES_WAL_TOPIC, ProduceWindow, Producer, ProducerRecord, ProfileRecord,
    ProfilesError, WalBatchError, WalSink, partition_key, write_batch_pipelined,
};

pub struct KafkaSink {
    pub(crate) producer: Arc<Producer>,
    pub(crate) topic: String,
}

impl KafkaSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>) -> Self {
        Self::with_topic(producer, PROFILES_WAL_TOPIC.to_owned())
    }

    #[must_use]
    pub fn with_topic(producer: Arc<Producer>, topic: String) -> Self {
        Self { producer, topic }
    }

    /// Hands one record to the producer and returns the future for its ack.
    ///
    /// The two halves are separate because only the first decides order.
    /// `Producer::send` appends the record to the partition accumulator, and
    /// the returned future resolves when the broker acks it.
    async fn enqueue(
        &self,
        rec: ProfileRecord,
    ) -> Result<impl Future<Output = Result<(), ProfilesError>> + use<>, ProfilesError> {
        let key = partition_key(&rec.tenant, rec.series_fingerprint());
        let value = rec.encode()?;
        // Inject the current span's W3C trace context (traceparent/tracestate)
        // as Kafka record headers so the block-builder consumer can re-parent
        // its block-build span onto this ingest span, stitching one distributed
        // trace across the WAL. Additive: empty when no active/sampled span.
        let headers = krabka_telemetry::propagation::current_trace_headers()
            .into_iter()
            .map(|(k, v)| Header {
                key: k,
                value: Some(Bytes::from(v.into_bytes())),
            })
            .collect();
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
                headers,
                ..Default::default()
            })
            .await;
        Ok(async move {
            ack.await
                .map_err(|err| ProfilesError::Produce(err.to_string()))?
                .map_err(|err| ProfilesError::Produce(err.to_string()))?;
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
        self.enqueue(rec).await?.await
    }

    async fn append_batch(
        &self,
        records: Vec<ProfileRecord>,
    ) -> Result<(), WalBatchError<ProfilesError>> {
        write_batch_pipelined(records, ProduceWindow::default(), |rec| self.enqueue(rec)).await
    }
}
