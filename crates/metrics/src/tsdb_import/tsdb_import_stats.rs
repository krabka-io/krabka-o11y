use super::{Deserialize, Serialize};

/// What one decoded block holds, after tombstones.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TsdbImportStats {
    pub series: u64,
    pub chunks: u64,
    pub float_samples: u64,
    pub histogram_samples: u64,
    /// Stale markers in histogram chunks, which the import stores as float
    /// stale markers so that the query path hides the series the same way.
    pub stale_histogram_markers: u64,
    /// Samples that the block's tombstones deleted.
    pub deleted_samples: u64,
}
