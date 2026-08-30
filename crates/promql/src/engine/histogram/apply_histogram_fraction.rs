use super::{BTreeMap, BTreeSet, ClassicBucket, InstantSample, Labels, Result, SampleValue, classic_histogram_fraction, float_sample_value, labels_key, labels_without_metric_and_label, labels_without_metric_name, native_histogram_fraction, parse_classic_bucket_bound, record_metric_name, warn_mixed_histograms};

/// Applies `histogram_fraction(lower, upper, v)` to an instant vector `v`.
///
/// This function mirrors `PromqlEngine::eval_histogram_fraction_call` exactly.
/// Native-histogram rows fold through [`native_histogram_fraction`] and keep the
/// source timestamp. This function groups classic `<metric>_bucket{le}` float
/// rows by labelset and drops `__name__` and `le` from the group. Each group
/// then folds through [`classic_histogram_fraction`] and carries `time_ms`.
///
/// This function drops a labelset that carries both a classic and a native
/// histogram from the output. It raises the
/// `MixedClassicNativeHistogramsWarning` through the in-scope annotation sink,
/// exactly as the interpreter does. The interpreter and the operator path share
/// this function, so the two are parity-exact.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(crate) fn apply_histogram_fraction(
    lower: f64,
    upper: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut native_samples = BTreeMap::new();
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(hist) = sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_fraction(lower, upper, &hist)),
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
                    value: SampleValue::Float(classic_histogram_fraction(
                        lower,
                        upper,
                        &mut buckets,
                    )),
                })
            }),
    );
    Ok(out)
}
