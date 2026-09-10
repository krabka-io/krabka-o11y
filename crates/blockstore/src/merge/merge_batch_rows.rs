/// Rows per batch a [`SortedMerge`](super::SortedMerge) emits.
///
/// This is the merge's resident cost, paid once per input and once for the
/// batch being assembled, so it is what makes a compaction's memory a function
/// of its output. Eight thousand rows is the size Arrow's own operators
/// default to: large enough that the per-batch overhead -- a comparison scan
/// over the key columns and a concatenation of the slices -- disappears
/// against the rows, and small enough that a few of them are nothing beside
/// the row group the Parquet writer is buffering anyway.
pub const MERGE_BATCH_ROWS: usize = 8_192;
