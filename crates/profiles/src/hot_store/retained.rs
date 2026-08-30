use super::ProfileRecord;

/// A source record that the store retains for retention bookkeeping and
/// rebuilds.
pub(crate) struct Retained {
    /// Newest sample timestamp (ms) carried by this record.
    pub(crate) max_ts_ms: i64,
    pub(crate) record: ProfileRecord,
}
