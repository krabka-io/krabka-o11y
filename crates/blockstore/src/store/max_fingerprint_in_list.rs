/// Largest resolved fingerprint set still expressed as a `series_fingerprint IN
/// (…)` scan predicate.
///
/// An `IN` list is exact, so the Parquet scan returns only the rows the caller
/// asked for. Its cost is not free, though: `PruningPredicate` rewrites an
/// `IN` list into one `min <= v AND max >= v` disjunct per value and evaluates
/// that against every row group of every candidate block, so a selector
/// matching a hundred thousand series would spend more time pruning than
/// reading. Above this many fingerprints the predicate degrades to the
/// `[first, last]` bound instead — a superset, which the callers already
/// re-filter through their `labels_by_fp` lookup.
pub(crate) const MAX_FINGERPRINT_IN_LIST: usize = 1_024;
