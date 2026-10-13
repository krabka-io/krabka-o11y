use super::{MetricBucket, MetricPoint, Result};

pub(crate) fn histogram_points(
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    mut value: impl FnMut(&MetricBucket) -> Result<f64>,
) -> Result<Vec<MetricPoint>> {
    buckets
        .iter()
        .enumerate()
        .map(|indexed_bucket| {
            let idx = indexed_bucket.0;
            let bucket = indexed_bucket.1;
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            Ok(MetricPoint::new(ts, value(bucket)?))
        })
        .collect()
}
