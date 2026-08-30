use super::*;

/// Result-value column that the aggregation projection emits.
///
/// The column reuses the leaf/rate `value` name, so the engine's batch-label
/// reader treats every other `Utf8` column as a grouping label.
pub const AGGREGATE_VALUE_COLUMN: &str = VALUE_COLUMN;
