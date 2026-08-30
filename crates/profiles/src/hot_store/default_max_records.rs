/// Hard cap on the number of retained source records, so a burst of same-instant
/// samples still cannot grow the hot store unboundedly.
pub(crate) const DEFAULT_MAX_RECORDS: usize = 1_000_000;
