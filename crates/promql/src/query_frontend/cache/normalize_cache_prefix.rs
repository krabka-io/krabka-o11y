use super::*;

pub(crate) fn normalize_cache_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "query-cache".to_string()
    } else {
        trimmed.to_string()
    }
}
