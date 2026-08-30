use super::{Deserialize, Serialize};

/// One series label set persisted in a compaction manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionSeriesLabels {
    pub fingerprint: u64,
    pub labels: krabka_blockstore::Labels,
}
