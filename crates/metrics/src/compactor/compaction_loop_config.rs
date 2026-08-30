use super::*;

/// Runtime knobs for the compactor polling loop.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionLoopConfig {
    pub wal_topic: String,
    pub poll_timeout: Time,
    /// Flush the accumulated buffer once this many WAL records are buffered.
    pub flush_max_rows: usize,
    /// Flush the accumulated buffer once its oldest record reaches this age.
    pub flush_max_age: Time,
}
