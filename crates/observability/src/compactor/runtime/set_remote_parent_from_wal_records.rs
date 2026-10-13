use super::{KafkaWalHeader, KafkaWalRecord};

/// Re-parents `span` from Kafka headers carrying a trace context.
///
/// The Kafka client exposes headers through its own representation. Callers
/// can first convert those external headers into [`KafkaWalHeader`] records,
/// keeping the conversion to the propagation API at this boundary.
pub fn set_remote_parent_from_kafka_headers(span: &tracing::Span, headers: &[KafkaWalHeader]) {
    if !headers.iter().any(|header| header.key == "traceparent") {
        return;
    }
    krabka_telemetry::propagation::set_remote_parent(
        span,
        headers.iter().map(|header| {
            (
                header.key.as_str(),
                header.value.as_deref().unwrap_or(&[][..]),
            )
        }),
    );
}

/// Re-parents `span` into the trace carried by the first record whose headers
/// include a `traceparent`.
///
/// There is one span per poll batch rather than one per record, so the first
/// record carrying a trace context stands for the batch. A record without one
/// is skipped rather than used: extracting from its headers would find no
/// context and leave the batch in a trace of its own.
pub(crate) fn set_remote_parent_from_wal_records(span: &tracing::Span, records: &[KafkaWalRecord]) {
    let Some(parent) = records
        .iter()
        .find(|rec| rec.headers.iter().any(|h| h.key == "traceparent"))
    else {
        return;
    };
    set_remote_parent_from_kafka_headers(span, &parent.headers);
}
