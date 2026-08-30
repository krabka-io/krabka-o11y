use super::HeaderMap;

/// Tenant label for the ingest span. It falls back to `"unknown"` when the
/// `X-Scope-OrgID` header is absent or non-ASCII. This label is for the span
/// only and never rejects the request. Validation stays in
/// `tenant_from_headers`.
pub(crate) fn tenant_for_span(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}
