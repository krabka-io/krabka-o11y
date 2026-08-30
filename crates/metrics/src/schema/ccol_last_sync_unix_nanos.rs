use super::*;

/// Clock reading last-valid-reference column in epoch nanoseconds (`Int64`).
///
/// A `PromQL` query computes the holdover duration from this column alone.
pub const CCOL_LAST_SYNC_UNIX_NANOS: &str = "last_sync_unix_nanos";
