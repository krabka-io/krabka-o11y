use super::{
    BTreeMap, CompareGroup, META_TYPE_KEY, MetricsRange, TraceMetricSeries, compare_points,
};

pub(crate) fn compare_total_series(
    group: CompareGroup,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![(
            META_TYPE_KEY.to_string(),
            group.total_meta_type().to_string(),
        )],
        label_types: BTreeMap::default(),
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}
