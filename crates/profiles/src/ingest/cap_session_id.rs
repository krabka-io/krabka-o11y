use super::{Labels, Limits, fnv1a, replace_label};

/// Cap the cardinality of `__session_id__` with a stable modulo hash.
///
/// A `max_session_id_cardinality` of zero is unlimited, as in Pyroscope, so the
/// session id passes through untouched.
pub fn cap_session_id(labels: &mut Labels, limits: &Limits) {
    let buckets = limits.max_session_id_cardinality;
    if buckets == 0 {
        return;
    }
    let Some(raw) = labels.get("__session_id__").map(str::to_owned) else {
        return;
    };
    let bucket = fnv1a(raw.as_bytes()) % buckets;
    replace_label(labels, "__session_id__", &bucket.to_string());
}
