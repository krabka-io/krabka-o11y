use super::SortOptions;

/// Rows per Parquet row group in a Krabka block.
///
/// The default is larger than most blocks and would leave nothing to prune.
pub const BLOCK_ROW_GROUP_ROWS: usize = 100_000;

/// The zstd level every block is compressed at.
///
/// Level 1 is zstd's fastest setting and still compresses sorted blocks well.
pub const BLOCK_ZSTD_LEVEL: i32 = 1;

/// How the declared sort key orders rows: ascending, nulls first.
pub(crate) const SORT_KEY_OPTIONS: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};
