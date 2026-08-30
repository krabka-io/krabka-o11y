use super::*;

pub(crate) fn histogram_points(
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    mut value: impl FnMut(&MetricBucket) -> Result<f64>,
) -> Result<Vec<(i64, f64)>> {
    buckets
        .iter()
        .enumerate()
        .map(|(idx, bucket)| {
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            Ok((ts, value(bucket)?))
        })
        .collect()
}
