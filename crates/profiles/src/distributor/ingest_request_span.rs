use super::{PROFILES_WAL_TOPIC, TenantId, ingest_span_tenant};

/// Opens the one server span of an ingest request.
///
/// `krabka.ingest.samples` stays empty until the request body has run and
/// `record_ingest_outcome` fills in the item count.
pub(crate) fn ingest_request_span(tenant: Option<&TenantId>, bytes: u64) -> tracing::Span {
    tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        krabka.tenant = ingest_span_tenant(tenant),
        krabka.ingest.samples = tracing::field::Empty,
        krabka.ingest.bytes = bytes,
    )
}
