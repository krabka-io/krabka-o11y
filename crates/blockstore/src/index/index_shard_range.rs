use super::{Deserialize, Serialize};

/// The inclusive time span one index shard object covers.
///
/// Shards sit on a per-tenant grid: a shard's span is `[k * width, k * width +
/// width - 1]` for some `k`, so a block belongs to the shards its own span
/// crosses and an append touches only those, rather than rewriting the tenant.
/// The span is carried in the object's own path, which is what lets a query
/// decide from a listing alone which shards it has to read.
///
/// The bounds are plain `i64` ticks with no unit attached, because the shared
/// index has none: the metrics path counts milliseconds and the profiles path
/// nanoseconds through the same structure. This is why the logs path's
/// `TimeRange`, whose fields are named `start_ns`, is not reused here.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct IndexShardRange {
    pub start: i64,
    pub end: i64,
}

impl IndexShardRange {
    #[must_use]
    pub const fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn overlaps(self, min_ts: i64, max_ts: i64) -> bool {
        self.start <= max_ts && self.end >= min_ts
    }
}
