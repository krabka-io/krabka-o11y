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
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
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
        ack.await
            .map_err(|err| TracesError::Produce(err.to_string()))?
            .map_err(|err| TracesError::Produce(err.to_string()))?;
        Ok(())
    }
}
