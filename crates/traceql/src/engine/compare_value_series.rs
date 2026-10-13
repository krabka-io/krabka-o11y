use super::{
    BTreeMap, CompareGroup, META_TYPE_KEY, MetricsRange, TraceMetricSeries, compare_points,
};

pub(crate) fn compare_value_series(
    group: CompareGroup,
    attr_key: &str,
    value: &str,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![
            super::MetricLabel::new(META_TYPE_KEY, group.meta_type()),
            super::MetricLabel::new(attr_key, value),
        ],
        label_types: BTreeMap::default(),
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}
