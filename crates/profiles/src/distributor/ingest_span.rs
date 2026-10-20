use super::{PROFILES_WAL_TOPIC, TenantId};

/// Gives the tenant label for the ingest tracing span.
///
/// Unresolved tenants use a fixed label that cannot be a real tenant id.
pub(crate) fn ingest_span_tenant(tenant: Option<&TenantId>) -> &str {
    tenant.map_or("<unresolved>", TenantId::as_str)
}

/// Opens the server span for an ingest request.
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
