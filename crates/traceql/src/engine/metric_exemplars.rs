use super::{MetricBucket, TraceMetricExemplar};

pub(crate) fn metric_exemplars(
    buckets: &[MetricBucket],
    max_exemplars: usize,
) -> Vec<TraceMetricExemplar> {
    buckets
        .iter()
        .flat_map(|bucket| bucket.exemplars.iter().cloned())
        .take(max_exemplars)
        .collect()
}
