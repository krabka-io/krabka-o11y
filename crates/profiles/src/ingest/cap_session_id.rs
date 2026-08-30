use super::{Labels, fnv1a, replace_label};

/// Cap the cardinality of `__session_id__` with a stable modulo hash.
pub fn cap_session_id(labels: &mut Labels, buckets: u64) {
    let Some(raw) = labels.get("__session_id__").map(str::to_owned) else {
        return;
    };
    let bucket = fnv1a(raw.as_bytes()) % buckets.max(1);
    replace_label(labels, "__session_id__", &bucket.to_string());
}
