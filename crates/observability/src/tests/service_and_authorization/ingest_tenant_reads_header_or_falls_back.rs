use super::*;

/// `ingest_tenant` returns a present non-empty `X-Scope-OrgID` verbatim,
/// but falls back to `"unknown"` when the header is missing or empty.
#[test]
pub(crate) fn ingest_tenant_reads_header_or_falls_back() {
    let mut present = HeaderMap::new();
    present.insert("X-Scope-OrgID", "acme".parse().unwrap());
    assert_eq!(ingest_tenant(&present), "acme");

    let missing = HeaderMap::new();
    assert_eq!(ingest_tenant(&missing), "unknown");

    let mut empty = HeaderMap::new();
    empty.insert("X-Scope-OrgID", "".parse().unwrap());
    assert_eq!(ingest_tenant(&empty), "unknown");
}
