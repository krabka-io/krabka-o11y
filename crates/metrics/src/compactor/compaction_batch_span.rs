use krabka_observability::{KafkaWalHeader, set_remote_parent_from_kafka_headers};

use super::{ConsumerRecord, TRACEPARENT};

/// Builds the per-poll-batch `metrics_compaction` consumer span and joins it to
/// the producer trace on a WAL record's `traceparent` header, if there is one.
///
/// There is ONE span per poll batch, and not one per record. `set_remote_parent`
/// does nothing when no polled record carries a valid trace context, so this
/// span is always safe to build. The first record with a `traceparent` header
/// anchors the parent.
pub(crate) fn compaction_batch_span(
    records: &[ConsumerRecord],
    wal_records: usize,
) -> tracing::Span {
    let span = tracing::info_span!(
        "metrics_compaction",
        otel.kind = "consumer",
        krabka.wal.records = wal_records,
    );
    if let Some(record) = records.iter().find(|record| {
        record
            .headers
            .iter()
            .any(|header| header.key == TRACEPARENT)
    }) {
        let headers = record
            .headers
            .iter()
            .map(|header| KafkaWalHeader {
                key: header.key.clone(),
                value: header.value.as_ref().map(|value| value.to_vec()),
            })
            .collect::<Vec<_>>();
        set_remote_parent_from_kafka_headers(&span, &headers);
    }
    span
}
