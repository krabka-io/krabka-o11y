use super::{ConsumerRecord, TRACEPARENT, set_remote_parent};

/// Builds the per-poll-batch `metrics_compaction` consumer span and joins it to
/// the producer trace on a WAL record's `traceparent` header, if there is one.
///
/// There is ONE span per poll batch, and not one per record. `set_remote_parent`
/// does nothing when no polled record carries a valid trace context, so this
/// span is always safe to build. The first record with a `traceparent` header
/// anchors the parent.
pub(crate) fn compaction_batch_span(records: &[ConsumerRecord], wal_records: usize) -> tracing::Span {
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
        set_remote_parent(
            &span,
            record.headers.iter().map(|header| {
                (
                    header.key.as_str(),
                    header.value.as_deref().unwrap_or(&[][..]),
                )
            }),
        );
    }
    span
}
