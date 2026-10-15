use super::*;

/// A validated Prometheus TSDB block, as rows ready for the block writer.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedTsdbBlock {
    pub rows: TenantCompactionRows,
    pub stats: TsdbImportStats,
}

/// The files of one Prometheus TSDB block directory, as uploaded.
#[derive(Clone, Copy, Debug)]
pub struct TsdbBlockFiles<'a> {
    pub index: &'a [u8],
    /// The `chunks/NNNNNN` segments, sorted by file name.
    pub chunk_segments: &'a [&'a [u8]],
    /// The `tombstones` file. A block without deletions may omit it.
    pub tombstones: Option<&'a [u8]>,
}

/// The parts of a block's `meta.json` that the import checks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TsdbBlockMeta {
    pub min_time: i64,
    /// The exclusive upper bound of the block's samples.
    pub max_time: i64,
    /// The `thanos.labels` external labels, if the block carries any.
    pub external_labels: BTreeMap<String, String>,
}

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
