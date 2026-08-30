use super::*;

pub struct ScanResult {
    pub ctx: SessionContext,
    pub span_table: String,
    /// Approximate decoded size of the scanned data that the store registers
    /// into `ctx`. This is the data the scan inspected, before the query
    /// filters it. The engine passes the value up to
    /// `SearchResponse::inspected`.
    pub inspected: ByteSize,
}
