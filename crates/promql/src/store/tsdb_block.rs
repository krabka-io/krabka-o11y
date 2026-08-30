use super::*;

/// One compacted TSDB block exposed by `/api/v1/status/tsdb/blocks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbBlock {
    pub id: String,
    pub min_time: i64,
    pub max_time: i64,
    pub num_samples: usize,
    pub num_series: usize,
}
