use super::*;

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
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
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
        ack.await
            .map_err(|err| ProfilesError::Produce(err.to_string()))?
            .map_err(|err| ProfilesError::Produce(err.to_string()))?;
        Ok(())
    }
}
