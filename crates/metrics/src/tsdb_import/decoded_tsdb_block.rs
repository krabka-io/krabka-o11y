use super::{TenantCompactionRows, TsdbImportStats};

/// A validated Prometheus TSDB block, as rows ready for the block writer.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedTsdbBlock {
    pub rows: TenantCompactionRows,
    pub stats: TsdbImportStats,
}
