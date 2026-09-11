use super::{
    Arc, Bytes, Future, ProduceError, ProduceWindow, Producer, WalBatchError, WalRecord, WalSink,
    current_trace_headers, wal_producer_record, write_batch_pipelined,
};

/// Producer-backed metrics WAL sink.
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
        key: Bytes,
        record: WalRecord,
    ) -> Result<impl Future<Output = Result<(), ProduceError>> + use<>, ProduceError> {
        let value = record
            .encode()
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        // Inject the current ingest span's W3C trace context into the WAL record
        // headers so the downstream compactor can stitch its `metrics_compaction`
        // span onto this producer's trace. Additive: it only appends the
        // traceparent/tracestate headers, and is an empty `Vec` (no-op) when no
        // span is active or OTLP is disabled.
        let ack = self
            .producer
            .send(wal_producer_record(key, value, current_trace_headers()))
            .await;
        Ok(async move {
            ack.await
                .map_err(|error| ProduceError::Append(error.to_string()))?
                .map_err(|error| ProduceError::Append(error.to_string()))?;
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.enqueue(key, record).await?.await
    }

    async fn append_batch(
        &self,
        records: Vec<(Bytes, WalRecord)>,
    ) -> Result<(), WalBatchError<ProduceError>> {
        write_batch_pipelined(records, ProduceWindow::default(), |(key, record)| {
            self.enqueue(key, record)
        })
        .await
    }
}
