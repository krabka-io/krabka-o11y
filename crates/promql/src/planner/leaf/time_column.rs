
/// Leaf-batch column with the per-sample timestamp in epoch milliseconds.
///
/// This column is the time index of the operator chain, so
/// [`InstantManipulate`] rewrites it to the grid eval timestamp on output.
pub const TIME_COLUMN: &str = "timestamp";
