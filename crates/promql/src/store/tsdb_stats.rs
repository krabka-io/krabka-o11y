use super::{NamedTsdbStat, TsdbHeadStats};

/// Tenant-scoped TSDB status statistics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsdbStats {
    pub head_stats: TsdbHeadStats,
    pub series_count_by_metric_name: Vec<NamedTsdbStat>,
    pub label_value_count_by_label_name: Vec<NamedTsdbStat>,
    pub memory_in_bytes_by_label_name: Vec<NamedTsdbStat>,
    pub series_count_by_label_value_pair: Vec<NamedTsdbStat>,
}

#[cfg(test)]
impl TsdbStats {
    /// The statistics of a tenant with no series.
    pub(crate) fn empty() -> Self {
        Self {
            head_stats: TsdbHeadStats {
                num_series: 0,
                num_samples: 0,
                num_chunks: 0,
                min_time: 0,
                max_time: 0,
            },
            series_count_by_metric_name: Vec::new(),
            label_value_count_by_label_name: Vec::new(),
            memory_in_bytes_by_label_name: Vec::new(),
            series_count_by_label_value_pair: Vec::new(),
        }
    }
}
