use std::future::Future;

use super::*;

/// Kafka-backed WAL sink.
pub struct KafkaSink {
    pub(crate) producer: Arc<Producer>,
}

impl KafkaSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>) -> Self {
        Self { producer }
    }

    /// Hands one record to the producer and returns the future for its ack.
    ///
    /// The two halves are separate because only the first decides order.
    /// `Producer::send` appends the record to the partition accumulator, and
    /// the returned future resolves when the broker acks it.
    async fn enqueue(
        &self,
        rec: SpanRecord,
    ) -> Result<impl Future<Output = Result<(), TracesError>> + use<>, TracesError> {
        let key = partition_key(&rec.span.trace_id);
        let value = Bytes::from(rec.encode()?);
        // Inject the current ingest span's W3C trace context onto the WAL record
        // so the block-builder (WAL consumer) can continue the same distributed
        // trace. Empty when there is no active/sampled span, so this is additive.
        let headers = krabka_telemetry::propagation::current_trace_headers()
            .into_iter()
            .map(|(key, value)| Header {
                key,
                value: Some(Bytes::from(value.into_bytes())),
            })
            .collect();
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: TRACES_WAL_TOPIC.to_string(),
                key: Some(key),
                value: Some(value),
                headers,
                ..ProducerRecord::default()
            })
            .await;
        Ok(async move {
            ack.await
                .map_err(|err| TracesError::Produce(err.to_string()))?
                .map_err(|err| TracesError::Produce(err.to_string()))?;
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.enqueue(rec).await?.await
    }

    async fn append_batch(
        &self,
        records: Vec<SpanRecord>,
    ) -> Result<(), WalBatchError<TracesError>> {
        write_batch_pipelined(records, ProduceWindow::default(), |rec| self.enqueue(rec)).await
    }
}
