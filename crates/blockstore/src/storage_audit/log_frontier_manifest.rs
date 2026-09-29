use super::{BTreeMap, Deserialize};

/// The logs compaction frontier, as `krabka-observability` persists it at
/// `index/logs/compaction-frontier.json`.
///
/// The audit reads only the fields it compares. The blockstore does not
/// depend on the logs crate, so the shape is restated here.
#[derive(Debug, Deserialize)]
pub struct LogFrontierManifest {
    pub version: u32,
    /// The last compacted WAL offset of each partition, inclusive.
    pub partition_offsets: BTreeMap<i32, i64>,
}

impl LogFrontierManifest {
    /// The only frontier version this build reads.
    pub const VERSION: u32 = 1;
}
