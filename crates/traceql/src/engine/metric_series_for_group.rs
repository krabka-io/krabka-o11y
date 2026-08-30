use super::*;

pub(crate) fn metric_series_for_group(
    labels: Vec<(String, String)>,
    buckets: Vec<MetricBucket>,
    metric: &MetricPlan,
    start_ns: i64,
    step_ns: i64,
    step: Time,
    metric_policy: (usize, &[Time]),
) -> Result<Vec<TraceMetricSeries>> {
    let (max_exemplars, histogram_buckets) = metric_policy;
    let exemplars = metric_exemplars(&buckets, max_exemplars);
    if matches!(metric.function, MetricFunction::QuantileOverTime) {
        return metric
            .quantiles
            .iter()
            .map(|quantile| {
                let mut labels = labels.clone();
                labels.insert(0, ("p".into(), quantile_label(*quantile)));
                let points = buckets
                    .iter()
                    .enumerate()
                    .map(|(idx, bucket)| {
                        let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
                        Ok((ts, bucket.quantile(*quantile)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TraceMetricSeries {
                    labels,
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
            histogram_buckets,
        );
    }

    let points = buckets
        .into_iter()
        .enumerate()
        .map(|(idx, bucket)| {
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            let value = match metric.function {
                MetricFunction::Rate => f64_from_u64(bucket.count)? / step.secs_f64(),
                MetricFunction::CountOverTime => f64_from_u64(bucket.count)?,
                MetricFunction::SumOverTime => bucket.sum,
                MetricFunction::AvgOverTime => bucket.average()?,
                MetricFunction::MinOverTime => bucket.min.unwrap_or(0.0),
                MetricFunction::MaxOverTime => bucket.max.unwrap_or(0.0),
                MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime => {
                    unreachable!("handled above")
                }
            };
            Ok((ts, value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![TraceMetricSeries {
        labels,
        points,
        exemplars,
    }])
}
