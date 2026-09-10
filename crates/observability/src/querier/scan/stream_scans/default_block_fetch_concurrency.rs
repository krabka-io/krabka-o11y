use super::NonZeroUsize;

/// How many cold blocks a scan fetches at once when nothing configures it.
///
/// Every cold path -- stream and metric alike -- fetches its planned blocks in
/// batches of this size unless the querier's
/// `querier_cold_block_fetch_concurrency` setting says otherwise. The entry
/// points that take no querier state have no setting to read, so they read
/// this.
pub(crate) fn default_block_fetch_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(8).expect("default block fetch concurrency is nonzero")
}
