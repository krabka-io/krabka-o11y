use super::*;

/// Gives the best-effort tenant label for the ingest tracing span.
///
/// This function reads the `X-Scope-OrgID` header directly and does not
/// validate it. An absent or empty header gives `"unknown"`. The label only
/// tags the span. [`tenant_from_headers`] separately resolves and validates the
/// tenant that storage uses.
pub(crate) fn ingest_span_tenant(headers: &HeaderMap) -> String {
    headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}
