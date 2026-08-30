use super::{HeaderMap, TENANT_HEADER};

pub(crate) fn tenant(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}
