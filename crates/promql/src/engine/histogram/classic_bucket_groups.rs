use super::{
    BTreeMap, BTreeSet, ClassicBucket, InstantSample, Labels, Result, classic_bucket_bound,
    float_sample_value, labels_key, labels_without_label, labels_without_metric_name,
    record_metric_name, warn_mixed_histograms,
};

/// Classic `<metric>_bucket{le}` buckets, grouped by the labelset key without
/// `__name__` and `le`.
pub(super) type ClassicBucketGroups = BTreeMap<String, (Labels, Vec<ClassicBucket>)>;

/// Adds a classic float bucket row to its group and records its metric name.
///
/// A row without a usable `le` label is skipped.
///
/// # Errors
///
/// Returns [`crate::PromqlError`] for an unparseable `le` bound or a
/// non-float bucket count.
pub(super) fn group_classic_bucket_sample(
    groups: &mut ClassicBucketGroups,
    metric_names: &mut BTreeMap<String, String>,
    sample: &InstantSample,
) -> Result<()> {
    let Some(upper_bound) = classic_bucket_bound(&sample.labels) else {
        return Ok(());
    };
    let count = float_sample_value(sample)?;
    let labels = labels_without_label(&labels_without_metric_name(&sample.labels), "le");
    let key = labels_key(&labels);
    record_metric_name(metric_names, &key, &sample.labels);
    groups
        .entry(key)
        .or_insert_with(|| (labels, Vec::new()))
        .1
        .push(ClassicBucket { upper_bound, count });
    Ok(())
}

/// Returns the labelset keys that carry both a native histogram and a classic
/// bucket group, and raises `MixedClassicNativeHistogramsWarning` for them.
pub(super) fn find_mixed_histogram_keys<NativeSample>(
    native_samples: &BTreeMap<String, NativeSample>,
    groups: &ClassicBucketGroups,
    metric_names: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    warn_mixed_histograms(&mixed_histogram_keys, metric_names);
    mixed_histogram_keys
}
