use super::{BTreeMap, Deserialize};

/// The logs compaction frontier, as `krabka-observability` persists it at
/// `index/logs/compaction-frontier.json`.
///
/// The blockstore does not depend on the logs crate, so the shape is restated
/// here. It has every field of `CompactionFrontierManifest` in that crate, with
/// the same types, so a frontier that the querier refuses to load does not
/// decode here either.
#[derive(Debug, Deserialize)]
pub struct LogFrontierManifest {
    pub version: u32,
    /// The querier treats every hot-tail record at or before this instant,
    /// in nanoseconds, as compacted.
    pub compacted_through_ns: i64,
    /// The last compacted WAL offset of each partition, inclusive.
    pub partition_offsets: BTreeMap<i32, i64>,
}

impl LogFrontierManifest {
    /// The only frontier version this build reads.
    pub const VERSION: u32 = 1;
}
