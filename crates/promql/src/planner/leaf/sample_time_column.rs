
/// Leaf-batch column that keeps the original sample timestamp.
///
/// This column is not the time index, so the operator chain carries it through
/// unchanged with `take`. The engine then recovers the true timestamp of the
/// selected sample. The interpreter reports that timestamp as
/// `InstantSample.ts_ms`, and `timestamp()` reads it.
pub const SAMPLE_TIME_COLUMN: &str = "sample_timestamp";
