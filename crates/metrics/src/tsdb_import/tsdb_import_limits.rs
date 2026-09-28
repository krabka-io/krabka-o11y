/// Bounds that keep the memory of one TSDB block import finite.
///
/// The import holds the uploaded files and the decoded rows in memory, so each
/// bound caps one input that would otherwise grow the allocation without
/// limit. The decoder checks a count against its bound before it allocates for
/// it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsdbImportLimits {
    /// The sum of the index, chunk segment and tombstones sizes.
    pub max_block_bytes: u64,
    pub max_symbols: u64,
    pub max_series: u64,
    pub max_labels_per_series: u64,
    pub max_chunks_per_series: u64,
    /// The float and histogram samples of the whole block, before tombstones.
    pub max_samples: u64,
    /// The spans, buckets or custom bounds of one histogram side.
    pub max_histogram_buckets: u64,
}

impl Default for TsdbImportLimits {
    fn default() -> Self {
        Self {
            max_block_bytes: 1 << 30,
            max_symbols: 4_000_000,
            max_series: 1_000_000,
            max_labels_per_series: 256,
            max_chunks_per_series: 100_000,
            max_samples: 25_000_000,
            max_histogram_buckets: 1 << 16,
        }
    }
}

impl TsdbImportLimits {
    pub(crate) fn check(
        limit: &'static str,
        value: u64,
        max: u64,
    ) -> Result<(), super::TsdbImportError> {
        if value > max {
            return Err(super::TsdbImportError::LimitExceeded { limit, value, max });
        }
        Ok(())
    }
}
