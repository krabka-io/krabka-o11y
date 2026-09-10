use super::{ScanReport, SessionContext};

/// A registered scan: the context to query, the table name to query it by, and
/// what the scan could not read.
#[derive(Clone)]
pub struct BlockScan {
    /// The session the table is registered in.
    pub ctx: SessionContext,

    /// The registered table's name.
    pub table: String,

    /// What was skipped, if anything. See [`ScanReport`].
    pub report: ScanReport,
}
