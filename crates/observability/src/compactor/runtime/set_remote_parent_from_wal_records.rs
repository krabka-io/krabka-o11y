use super::KafkaWalRecord;

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
    krabka_telemetry::propagation::set_remote_parent(
        span,
        parent
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
    );
}
