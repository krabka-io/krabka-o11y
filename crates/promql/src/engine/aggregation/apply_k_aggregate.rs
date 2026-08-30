use super::{
    BTreeMap, InstantSample, LabelModifier, SampleValue, TokenType, aggregate_labels,
    compare_k_aggregate_samples, labels_key,
};

/// Shared `topk`/`bottomk` core over an already-evaluated instant vector.
///
/// This function backs both the interpreter (`PromqlEngine::eval_k_aggregate`)
/// and the operator path (`PromqlEngine::plan_param_aggregate_expr`), so the two
/// are identical by construction once their inputs match. It groups the samples
/// by the `by`/`without` label set and sorts each group by value: highest first
/// for `topk`, lowest first for `bottomk`, with a `labels_key` tie-break. It
/// then clamps each group to `k` and returns the surviving original samples.
/// The labels, including `__name__`, the timestamp, and the value all stay
/// unchanged, because this is a selection and not a reduction. This function
/// skips histogram-typed samples, which carry no float to rank. A `k` of 0
/// returns the empty vector.
pub(crate) fn apply_k_aggregate(
    samples: Vec<InstantSample>,
    op: TokenType,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    if k == 0 {
        return Vec::new();
    }

    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            continue;
        }
        let labels = aggregate_labels(&sample.labels, modifier);
        groups.entry(labels_key(&labels)).or_default().push(sample);
    }

    let mut out = Vec::new();
    for mut group in groups.into_values() {
        group.sort_by(|left, right| compare_k_aggregate_samples(op, left, right));
        group.truncate(k.min(group.len()));
        out.extend(group);
    }
    out
}
