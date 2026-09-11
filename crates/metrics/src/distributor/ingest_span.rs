use super::{ByteSize, ByteSizeExt, TenantId, WAL_TOPIC};

/// Builds the per-request ingest span. This function declares
/// `krabka.ingest.series` empty, and `push_inner` records it after it decodes
/// the request body.
///
/// `tenant` is the resolved tenant, or `None` when the request names no usable
/// tenant. The span of such a request carries the tenant `unknown`. The label
/// is for the span only, and the push path rejects the request.
pub(crate) fn ingest_span(tenant: Option<&TenantId>, body_size: ByteSize) -> tracing::Span {
    let tenant = tenant.map_or("unknown", TenantId::as_str);
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
