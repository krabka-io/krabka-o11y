use super::*;

/// Make `span` a child of the distributed trace carried on any consumed
/// record's `traceparent` header.
///
/// This uses the first record that carries such a header. It does nothing when
/// no record carries one, for example when an unsampled ingest request produced
/// the records.
pub(crate) fn set_remote_parent_from_records(span: &tracing::Span, records: &[ConsumerRecord]) {
    let Some(record) = records
        .iter()
        .find(|record| record.headers.iter().any(|h| h.key == TRACEPARENT_HEADER))
    else {
        return;
    };
    krabka_telemetry::propagation::set_remote_parent(
        span,
        record
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
    );
}
