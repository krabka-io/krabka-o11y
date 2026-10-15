use super::{ByteSize, mebibytes};

/// Default width of one index shard, in the index's own timestamp ticks.
///
/// The shared index has no time unit of its own, so callers with another unit
/// can provide their own width when saving.
pub const DEFAULT_INDEX_SHARD_WIDTH: i64 = 24 * 60 * 60 * 1_000;

/// Most shards one tenant's index is cut into.
///
/// The width doubles until a tenant's blocks fit, so an incorrect default
/// width only makes shards coarser.
pub const MAX_INDEX_SHARDS_PER_TENANT: usize = 256;

/// Maximum byte size of one index shard object accepted by a load.
///
/// A load buffers an object before decoding it, so this cap prevents an
/// oversized or corrupt object from consuming unbounded memory.
pub const MAX_INDEX_SNAPSHOT_BYTES: ByteSize = mebibytes(256);
