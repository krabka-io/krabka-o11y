use super::*;

#[must_use]
pub fn index_snapshot_prefix_for_key(key: &str) -> String {
    let key = key.trim_matches('/');
    // Use a sibling prefix, not "{key}/snapshots": object stores allow that
    // shape, but filesystem-backed S3 services may already map the legacy key
    // to a directory containing retained physical object parts.
    if let Some(stem) = key.strip_suffix(".json") {
        format!("{stem}/snapshots")
    } else {
        format!("{key}.snapshots")
    }
}
