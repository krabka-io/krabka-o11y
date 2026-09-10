use super::{
    BTreeMap, BTreeSet, ClassicBucket, InstantSample, Labels, Result, SampleValue,
    classic_bucket_bound, classic_histogram_quantile, emit_info, emit_warning, float_sample_value,
    histogram_quantile_forced_monotonicity_info, invalid_quantile_warning, is_valid_quantile,
    labels_key, labels_without_label, labels_without_metric_and_label, labels_without_metric_name,
    native_histogram_quantile, record_metric_name, warn_mixed_histograms,
};

/// Prometheus. Both the `__name__` and `le` labels are dropped from every output
/// series. Classic output samples carry `time_ms`; native ones keep the source
/// sample timestamp.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(crate) fn apply_histogram_quantile(
    quantile: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    // `funcHistogramQuantile` warns about an out-of-range quantile before it
    // looks at a single sample, so an empty input vector still warns.
    if !is_valid_quantile(quantile) {
        emit_warning(invalid_quantile_warning(quantile));
    }
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
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
            let key = labels_key(&sample.labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_quantile(
                        quantile,
                        histogram,
                        sample.labels.get("__name__").unwrap_or(""),
                    )),
                },
            );
            continue;
        }
        let Some(upper_bound) = classic_bucket_bound(&sample.labels) else {
            continue;
        };
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        let key = labels_key(&labels_without_label(&sample.labels, "le"));
        record_metric_name(&mut metric_names, &key, &sample.labels);
        groups
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    warn_mixed_histograms(&mixed_histogram_keys, &metric_names);
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
                let (value, forced) = classic_histogram_quantile(quantile, &mut buckets);
                if forced {
                    emit_info(histogram_quantile_forced_monotonicity_info(
                        metric_names.get(&key).map_or("", String::as_str),
                    ));
                }
                Some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(value),
                })
            }),
    );
    Ok(out)
}
