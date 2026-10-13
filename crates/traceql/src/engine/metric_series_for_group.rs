use super::*;

pub(crate) fn metric_series_for_group(
    labels: MetricLabels,
    buckets: Vec<MetricBucket>,
    metric: &MetricPlan,
    grid: MetricGrid,
    step: Time,
    exemplar_range: ExemplarRange,
    metric_policy: MetricExemplarPolicy<'_>,
) -> Result<Vec<TraceMetricSeries>> {
    let start_ns = grid.start_ns;
    let step_ns = grid.step_ns;
    let exemplars = metric_exemplars(
        &buckets,
        metric_policy.max_exemplars_per_series,
        exemplar_range.start_ns,
        exemplar_range.end_ns,
    );
    if matches!(metric.function, MetricFunction::QuantileOverTime) {
        return metric
            .quantiles
            .iter()
            .map(|quantile| {
                let mut labels = labels.clone();
                labels
                    .labels
                    .insert(0, MetricLabel::new("p", quantile_label(*quantile)));
                let points = (0..buckets.len())
                    .map(|bucket_index| {
                        let ts =
                            start_ns + i64::try_from(bucket_index).unwrap_or(i64::MAX) * step_ns;
                        Ok(MetricPoint::new(
                            ts,
                            buckets[bucket_index].quantile(*quantile)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TraceMetricSeries {
                    label_types: labels.types.clone(),
                    labels: labels.labels.clone(),
                    points,
                    exemplars: exemplars.clone(),
                })
            })
            .collect();
    }
    if matches!(metric.function, MetricFunction::HistogramOverTime) {
        return histogram_series_for_group(
            labels,
            &buckets,
            start_ns,
            step_ns,
            &exemplars,
            metric_policy.histogram_buckets,
        );
    }

    let points = buckets
        .iter()
        .enumerate()
        .map(|indexed_bucket| {
            let bucket_index = indexed_bucket.0;
            let bucket = indexed_bucket.1;
            let ts = start_ns + i64::try_from(bucket_index).unwrap_or(i64::MAX) * step_ns;
            let value = match metric.function {
                MetricFunction::Rate => f64_from_u64(bucket.count)? / step.secs_f64(),
                MetricFunction::CountOverTime => f64_from_u64(bucket.count)?,
                MetricFunction::SumOverTime => {
                    if bucket.count == 0 {
                        f64::NAN
                    } else {
                        bucket.sum
                    }
                }
                MetricFunction::AvgOverTime => bucket.average(),
                MetricFunction::MinOverTime => bucket.min.unwrap_or(f64::NAN),
                MetricFunction::MaxOverTime => bucket.max.unwrap_or(f64::NAN),
                MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime => {
                    unreachable!("handled above")
                }
            };
            Ok(MetricPoint::new(ts, value))
        })
        .collect::<Result<Vec<_>>>()?;
    let points = points
        .into_iter()
        .filter(|point| !point.value.is_nan())
        .collect::<Vec<_>>();
    if points.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![TraceMetricSeries {
        label_types: labels.types.clone(),
        labels: labels.labels,
        points,
        exemplars,
    }])
}
