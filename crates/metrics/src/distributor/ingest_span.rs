use super::{HeaderMap, ByteSize, tenant_for_span, WAL_TOPIC, ByteSizeExt};

/// Builds the per-request ingest span. This function declares
/// `krabka.ingest.series` empty, and `push_inner` records it after it decodes
/// the request body.
pub(crate) fn ingest_span(headers: &HeaderMap, body_size: ByteSize) -> tracing::Span {
    let tenant = tenant_for_span(headers);
    tracing::info_span!(
        "metrics_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = WAL_TOPIC,
        krabka.tenant = %tenant,
        krabka.ingest.series = tracing::field::Empty,
        krabka.ingest.bytes = body_size.bytes_u64(),
    )
}
