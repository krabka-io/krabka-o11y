use super::{ByteSize, mebibytes};

/// Default memory budget for the Parquet footer cache of one
/// [`BlockStore`](crate::BlockStore).
///
/// A cache without a bound is a memory leak with extra steps, and the number
/// of blocks a querier touches has no ceiling: retention and cardinality both
/// grow, and a wide range query walks every block in its window. So the bound
/// is on bytes held, not on entries, and eviction is least-recently-used —
/// which matches how queries arrive, in bursts against a moving recent window.
///
/// 128 MiB is roughly a thousand footers for the blocks this store writes
/// (100k-row row groups, a few dozen columns, sorting columns and a `span_id`
/// bloom), and a footer with its page index is the largest of them. It is a
/// starting point, not a law: set it from configuration with
/// [`BlockStore::with_metadata_cache_max`](crate::BlockStore::with_metadata_cache_max).
pub const DEFAULT_BLOCK_METADATA_CACHE_MAX: ByteSize = mebibytes(128);
