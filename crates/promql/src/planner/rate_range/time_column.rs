use super::*;

/// Leaf-batch column that holds the per-sample timestamp in epoch milliseconds.
/// This column is the operator chain's time index. `RangeManipulate` reuses the
/// name for the scalar eval-timestamp column that it emits.
pub const TIME_COLUMN: &str = "timestamp";
