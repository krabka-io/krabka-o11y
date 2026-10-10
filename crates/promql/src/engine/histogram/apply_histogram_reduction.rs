use super::{
    BTreeMap, ClassicBucket, ClassicBucketGroups, InstantSample, NativeHistogram, Result,
    SampleValue, find_mixed_histogram_keys, group_classic_bucket_sample, labels_key,
    labels_without_metric_name, record_metric_name,
};

/// How [`apply_histogram_reduction`] folds each histogram to a float.
pub(crate) struct HistogramReducers<N, C> {
    /// Folds a native histogram, given its metric name.
    pub(crate) native: N,
    /// Folds a classic group's buckets, given its metric name.
    pub(crate) classic: C,
}

/// Reduces each native histogram and each classic `<metric>_bucket{le}` group
/// of an instant vector to one float, as `histogram_quantile` and
/// `histogram_fraction` do.
///
/// Native rows fold through `reducers.native(histogram, metric_name)` and
/// keep the source timestamp. Classic float rows group by labelset without
/// `__name__` and `le`; each group folds through
/// `reducers.classic(metric_name, buckets)` in group order and carries
/// `time_ms`. Every output drops `__name__`. A labelset that
/// carries both a classic and a native histogram is dropped, and raises the
/// `MixedClassicNativeHistogramsWarning`.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count.
pub(crate) fn apply_histogram_reduction<N, C>(
    samples: Vec<InstantSample>,
    time_ms: i64,
    reducers: HistogramReducers<N, C>,
) -> Result<Vec<InstantSample>>
where
    N: Fn(&NativeHistogram, &str) -> f64,
    C: FnMut(&str, &mut Vec<ClassicBucket>) -> f64,
{
    let HistogramReducers {
        native,
        mut classic,
    } = reducers;
    let mut groups = ClassicBucketGroups::new();
    let mut native_samples = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            // Series group by their labels WITHOUT `le` but WITH `__name__`,
            // which is the signature `resetHistograms` builds. Two metrics that
            // differ only in name therefore stay two groups, and their two
            // output samples -- which have both lost `__name__` -- collide as
            // Prometheus intends.
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native(
                        histogram,
                        sample.labels.get("__name__").unwrap_or(""),
                    )),
                    drop_name: true,
                },
            );
            continue;
        }
        group_classic_bucket_sample(&mut groups, &mut metric_names, &sample)?;
    }

    let mixed_histogram_keys = find_mixed_histogram_keys(&native_samples, &groups, &metric_names);
    let mut out = native_samples
        .into_iter()
        .filter_map(|(key, sample)| (!mixed_histogram_keys.contains(&key)).then_some(sample))
        .collect::<Vec<_>>();
    out.extend(
        groups
            .into_iter()
            .filter_map(|(key, (labels, mut buckets))| {
                if mixed_histogram_keys.contains(&key) {
                    return None;
                }
                let classic_value = classic(
                    metric_names.get(&key).map_or("", String::as_str),
                    &mut buckets,
                );
                Some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(classic_value),
                    drop_name: true,
                })
            }),
    );
    Ok(out)
}
