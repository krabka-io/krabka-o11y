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
