use super::{
    MetricBucket, MetricLabels, Result, Time, TraceMetricExemplar, TraceMetricSeries,
    f64_from_usize, histogram_points, quantile_label,
};

pub(crate) fn histogram_series_for_group(
    labels: MetricLabels,
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    exemplars: &[TraceMetricExemplar],
    _histogram_buckets: &[Time],
) -> Result<Vec<TraceMetricSeries>> {
    let (labels, label_types) = labels;
    let mut boundaries = buckets
        .iter()
        .flat_map(|bucket| bucket.values.iter().copied())
        .collect::<Vec<_>>();
    boundaries.sort_by(f64::total_cmp);
    boundaries.dedup_by(|lhs, rhs| lhs.to_bits() == rhs.to_bits());
    boundaries
        .into_iter()
        .map(|boundary| {
            let mut labels = labels.clone();
            labels.insert(0, ("__bucket".into(), quantile_label(boundary)));
            Ok(TraceMetricSeries {
                labels,
                label_types: label_types.clone(),
                points: histogram_points(buckets, start_ns, step_ns, |bucket| {
                    f64_from_usize(
                        bucket
                            .values
                            .iter()
                            .filter(|value| value.to_bits() == boundary.to_bits())
                            .count(),
                    )
                })?,
                exemplars: exemplars.to_owned(),
            })
        })
        .collect()
}
