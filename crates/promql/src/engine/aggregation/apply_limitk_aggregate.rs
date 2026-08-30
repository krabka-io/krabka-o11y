/// Shared experimental `limitk(k, v)` core over an already-evaluated instant
/// vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_limitk_aggregate`) and the operator path. It groups by
/// the `by`/`without` label set and keeps the first `k` members of each group in
/// a deterministic order: fingerprint first, then `labels_key`. This is exactly
/// what Prometheus' reproducible `limitk` does. The caller resolves `k` before
/// reaching here, and short-circuits `k==0` to the empty vector.
#[cfg(feature = "experimental-functions")]
pub(crate) fn apply_limitk_aggregate(
    samples: Vec<InstantSample>,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        let labels = aggregate_labels(&sample.labels, modifier);
        groups.entry(labels_key(&labels)).or_default().push(sample);
    }

    let mut out = Vec::new();
    for mut samples in groups.into_values() {
        samples.sort_by(|left, right| {
            left.labels
                .fingerprint()
                .cmp(&right.labels.fingerprint())
                .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
        });
        samples.truncate(k.min(samples.len()));
        out.extend(samples);
    }
    out
}
