use super::*;

pub(crate) fn histogram_series_for_group(
    labels: Vec<(String, String)>,
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    exemplars: &[TraceMetricExemplar],
    histogram_buckets: &[Time],
) -> Result<Vec<TraceMetricSeries>> {
    let mut out = Vec::with_capacity(histogram_buckets.len() + 3);
    for le in histogram_buckets {
        let le = f64_from_i64(le.nanos_i64());
        let mut labels = labels.clone();
        labels.insert(0, ("le".into(), quantile_label(le)));
        out.push(TraceMetricSeries {
            labels,
            points: histogram_points(buckets, start_ns, step_ns, |bucket| {
                f64_from_usize(bucket.values.iter().filter(|value| **value <= le).count())
            })?,
            exemplars: exemplars.to_owned(),
        });
    }

    let mut inf_labels = labels.clone();
    inf_labels.insert(0, ("le".into(), "+Inf".into()));
    out.push(TraceMetricSeries {
        labels: inf_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| {
            f64_from_u64(bucket.count)
        })?,
        exemplars: exemplars.to_owned(),
    });

    let mut sum_labels = labels.clone();
    sum_labels.insert(0, ("__metric__".into(), "sum".into()));
    out.push(TraceMetricSeries {
        labels: sum_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| Ok(bucket.sum))?,
        exemplars: Vec::new(),
    });

    let mut count_labels = labels;
    count_labels.insert(0, ("__metric__".into(), "count".into()));
    out.push(TraceMetricSeries {
        labels: count_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| {
            f64_from_u64(bucket.count)
        })?,
        exemplars: Vec::new(),
    });

    Ok(out)
}
