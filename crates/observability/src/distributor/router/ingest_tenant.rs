use super::*;

/// Tenant for an ingest request, from `X-Scope-OrgID`. It falls back to
/// `"unknown"` when the header is missing, non-UTF-8, or empty.
///
/// The value only labels the ingest span and the per-tenant metric. The WAL
/// records carry their own per-record tenant, so a permissive fallback here
/// never affects storage.
pub(crate) fn ingest_tenant(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|v| v.to_str().ok())
        .filter(|t| !t.is_empty())
        .unwrap_or("unknown")
        .to_string()
}
