use super::{HeaderMap, TENANT_HEADER};

pub(crate) fn tenant(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}
