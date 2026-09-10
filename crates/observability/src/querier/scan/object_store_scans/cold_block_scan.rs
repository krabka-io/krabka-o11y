use super::{Arc, NonZeroUsize, ObjectPath, ObjectStore};

/// Where a cold scan reads its planned blocks from, and how many at a time.
///
/// The store and the prefix always travel together -- neither addresses a
/// block without the other -- and the fetch concurrency is the only other
/// thing a cold scan needs that a local one does not. Carrying the three as
/// one value keeps the entry points below the argument count the local paths
/// already sit at.
pub(crate) struct ColdBlockScan<'a> {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: &'a ObjectPath,
    /// How many blocks are fetched at once. One object-store round trip per
    /// block, so this is what keeps a many-block range query off a serial
    /// chain of them.
    pub(crate) block_fetch_concurrency: NonZeroUsize,
}
