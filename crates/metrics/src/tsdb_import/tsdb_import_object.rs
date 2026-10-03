use super::{Deserialize, MetricBlockKind, Serialize};

/// One Parquet block that an import publishes, with its `.index` manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TsdbImportObject {
    pub kind: MetricBlockKind,
    pub block_key: String,
    pub index_key: String,
    pub rows: u64,
}
