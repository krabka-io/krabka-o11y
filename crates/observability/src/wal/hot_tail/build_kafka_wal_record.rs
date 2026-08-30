use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn build_kafka_wal_record(
    topic: impl Into<String>,
    record: &WalLogRecord,
) -> Result<ProducerRecord, WalSinkError> {
    let fingerprint = series_fingerprint(&record.labels);
    let mut headers = vec![
        ProducerHeader {
            key: "krabka-wal-record-type".to_string(),
            value: Some(Bytes::from_static(b"log")),
        },
        ProducerHeader {
            key: "krabka-tenant".to_string(),
            value: Some(Bytes::from(record.tenant.clone())),
        },
    ];
    // Inject the current span's W3C trace context (`traceparent`/`tracestate`)
    // so the compactor can stitch its consume/compaction span onto the ingest
    // trace. Additive: the record body is unchanged, and this is a no-op when
    // there is no active/sampled span.
    for (key, value) in krabka_telemetry::propagation::current_trace_headers() {
        headers.push(ProducerHeader {
            key,
            value: Some(Bytes::from(value.into_bytes())),
        });
    }
    Ok(ProducerRecord {
        topic: topic.into(),
        partition: None,
        key: Some(Bytes::from(format!("{}:{fingerprint}", record.tenant))),
        value: Some(Bytes::from(serde_json::to_vec(record)?)),
        headers,
        timestamp_ms: Some(record.timestamp_ns / 1_000_000),
    })
}
