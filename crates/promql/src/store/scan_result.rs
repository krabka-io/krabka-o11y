use super::*;

/// A leaf scan result with up to two `DataFusion` tables registered.
pub struct ScanResult {
    pub ctx: SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,
}
