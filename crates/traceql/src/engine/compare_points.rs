use super::{MetricPoint, MetricsRange, f64_from_u64};

pub(crate) fn compare_points(buckets: &[u64], range: MetricsRange) -> Vec<MetricPoint> {
    buckets
        .iter()
        .enumerate()
        .map(|indexed_count| {
            let idx = indexed_count.0;
            let count = indexed_count.1;
            let ts = range.output_start.0 + i64::try_from(idx).unwrap_or(i64::MAX) * range.step.0;
            MetricPoint::new(ts, f64_from_u64(*count).unwrap_or(0.0))
        })
        .collect()
}
