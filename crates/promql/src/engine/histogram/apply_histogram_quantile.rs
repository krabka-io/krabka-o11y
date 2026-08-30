use super::{InstantSample, Result, BTreeMap, Labels, ClassicBucket, SampleValue, labels_without_metric_name, labels_key, record_metric_name, native_histogram_quantile, parse_classic_bucket_bound, float_sample_value, labels_without_metric_and_label, BTreeSet, warn_mixed_histograms, classic_histogram_quantile};

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
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut native_samples = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_quantile(quantile, histogram)),
                },
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        let key = labels_key(&labels);
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
                (!mixed_histogram_keys.contains(&key)).then_some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(classic_histogram_quantile(quantile, &mut buckets)),
                })
            }),
    );
    Ok(out)
}
