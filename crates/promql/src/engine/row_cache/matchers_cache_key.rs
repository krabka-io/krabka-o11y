use super::*;

/// Deterministic cache key for a matcher set. `LabelMatcher` is not `Hash`, but
/// its `Debug` output is stable and uniquely identifies the (name, op, value)
/// triples in order. This is enough, because the same selector returns the same
/// matcher list at every step of a range query.
pub(crate) fn matchers_cache_key(matchers: &[LabelMatcher]) -> String {
    format!("{matchers:?}")
}
