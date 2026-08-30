use super::*;

/// Apply an optional object-store prefix to a raw traces object key.
#[must_use]
pub fn prefixed_object_key(prefix: &str, key: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let key = key.trim_start_matches('/');
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}/{key}")
    }
}
