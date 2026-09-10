/// Rows per Parquet row group in a Krabka block.
///
/// The parquet default is 1,048,576, which is more rows than a Krabka block
/// usually holds -- so the default makes every block a single row group and
/// leaves `DataFusion`'s row-group prune with nothing to prune. A hundred
/// thousand rows is small enough that a block cuts into tens of groups and a
/// selective predicate reads one of them, and large enough that the per-group
/// footer metadata stays a rounding error against the data.
pub const BLOCK_ROW_GROUP_ROWS: usize = 100_000;
