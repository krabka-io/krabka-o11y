/// Rebuild the queryable store only once evictions reach `1 / FACTOR` of the
/// retained window.
///
/// A rebuild is O(window). A deferred rebuild therefore amortizes the cost to
/// O(1) per append in steady state. The price is that at most
/// `window / FACTOR` already-evicted records stay in the queryable store for a
/// time. This is bounded memory slack and not a correctness fault. Those rows
/// are real, only older than the strict horizon, and queries still filter by
/// timestamp.
pub(crate) const REBUILD_AMORTIZE_FACTOR: usize = 8;
