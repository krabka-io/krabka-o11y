use super::*;

/// Producer-backed metrics WAL sink.
pub struct KafkaSink {
    pub(crate) producer: Arc<Producer>,
}

impl KafkaSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>) -> Self {
        Self { producer }
    }
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
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
        ack.await
            .map_err(|error| ProduceError::Append(error.to_string()))?
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        Ok(())
    }
}
