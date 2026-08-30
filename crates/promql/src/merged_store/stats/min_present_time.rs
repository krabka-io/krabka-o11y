use super::*;

/// Combines the head min-time of two stores with explicit presence flags.
///
/// A store reports emptiness with `None`, not with a `0` min-time, and the
/// caller threads a has-data flag through for this. This function keeps a
/// legitimate `min_time == 0` from a store that does hold samples, and never
/// reads that value as an empty store.
pub(crate) fn min_present_time(left: Option<i64>, right: Option<i64>) -> i64 {
    match (left, right) {
        (Some(left), Some(right)) => left.min(right),
        (Some(value), None) | (None, Some(value)) => value,
        (None, None) => 0,
    }
}
