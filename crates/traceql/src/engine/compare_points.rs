use super::{MetricsRange, f64_from_u64};

pub(crate) fn compare_points(buckets: &[u64], range: MetricsRange) -> Vec<(i64, f64)> {
    buckets
        .iter()
        .enumerate()
        .map(|(idx, count)| {
            let ts = range.output_start.0 + i64::try_from(idx).unwrap_or(i64::MAX) * range.step.0;
            (ts, f64_from_u64(*count).unwrap_or(0.0))
        })
        .collect()
}
