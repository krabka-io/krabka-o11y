use std::collections::VecDeque;

use super::ProfileRecord;

/// A source record retained for retention bookkeeping and rebuilds.
pub(crate) struct Retained {
    /// Newest sample timestamp (ms) carried by this record.
    pub(crate) max_ts_ms: i64,
    pub(crate) record: ProfileRecord,
    pub(crate) position: Option<crate::wal::WalPosition>,
}

/// Retained source records plus the count evicted since the last rebuild.
#[derive(Default)]
pub(crate) struct RetainedState {
    pub(crate) records: VecDeque<Retained>,
    pub(crate) evicted_since_rebuild: usize,
}
