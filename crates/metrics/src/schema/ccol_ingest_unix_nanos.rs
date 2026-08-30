use super::*;

/// Ingest clock column in epoch nanoseconds (`Int64`).
///
/// The ingester stamps this column from its own clock when the row arrives.
/// The host does not supply it. The difference between this column and
/// [`CCOL_READING_UNIX_NANOS`] is a measured skew between two named hosts, and
/// no single exporter can compute that number.
pub const CCOL_INGEST_UNIX_NANOS: &str = "ingest_unix_nanos";
