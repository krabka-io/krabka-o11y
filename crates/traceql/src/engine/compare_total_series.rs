use super::{
    BTreeMap, CompareGroup, META_TYPE_KEY, MetricsRange, TraceMetricSeries, compare_points,
};

pub(crate) fn compare_total_series(
    group: CompareGroup,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![super::MetricLabel::new(
            META_TYPE_KEY,
            group.total_meta_type(),
        )],
        label_types: BTreeMap::default(),
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}
