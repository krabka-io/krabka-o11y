
/// Counts of what an [`InMemoryMetricStore::prune`] pass evicted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PruneStats {
    /// Float, histogram, and exemplar rows dropped because they fell out of the
    /// retention window.
    pub samples_dropped: usize,
    /// Series, counted by distinct fingerprint, that lost their last sample.
    /// The store removed them from the queryable index.
    pub series_dropped: usize,
}
