use super::*;

/// Prometheus-style head block stats for `/api/v1/status/tsdb`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbHeadStats {
    pub num_series: usize,
    pub num_samples: usize,
    pub num_chunks: usize,
    pub min_time: i64,
    pub max_time: i64,
}
