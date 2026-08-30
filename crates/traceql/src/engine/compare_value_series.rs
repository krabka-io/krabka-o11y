use super::{CompareGroup, META_TYPE_KEY, MetricsRange, TraceMetricSeries, compare_points};

pub(crate) fn compare_value_series(
    group: CompareGroup,
    attr_key: &str,
    value: &str,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![
            (META_TYPE_KEY.to_string(), group.meta_type().to_string()),
            (attr_key.to_string(), value.to_string()),
        ],
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}
