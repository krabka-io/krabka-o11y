/// Most shards one tenant's index is cut into.
///
/// The default width is a guess about the caller's time unit. A guess that
/// assumes milliseconds of a nanosecond timeline is wrong by six orders of
/// magnitude, and it would cut a single day into a million objects. The width
/// doubles until the tenant's blocks fit within this many shards, so a wrong
/// guess only makes the shards coarse. Each doubling keeps the grid aligned,
/// so every boundary of a wider shard is also a boundary of a narrower one.
pub const MAX_INDEX_SHARDS_PER_TENANT: usize = 256;
