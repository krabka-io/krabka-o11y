use super::{
    BTreeMap, InstantSample, LabelModifier, SampleValue, T_TOPK, TokenType, aggregate_labels,
    compare_k_aggregate_samples, emit_info, histogram_ignored_in_aggregation_info, labels_key,
};

/// Shared `topk`/`bottomk` core over an already-evaluated instant vector.
///
/// This function backs both the interpreter (`PromqlEngine::eval_k_aggregate`)
/// and the operator path (`PromqlEngine::plan_param_aggregate_expr`), so the two
/// are identical by construction once their inputs match. It groups the samples
/// by the `by`/`without` label set, partitions each group to retain its best
/// `k` samples, and sorts only those samples: highest first for `topk`, lowest
/// first for `bottomk`, with a lexical label tie-break. Selection takes O(n)
/// comparisons per group, followed by O(k log k) sorting.
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

    let mut groups = BTreeMap::<String, Vec<(usize, InstantSample)>>::new();
    for (position, sample) in samples.into_iter().enumerate() {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            emit_info(histogram_ignored_in_aggregation_info(k_aggregate_name(op)));
            continue;
        }
        let labels = aggregate_labels(&sample.labels, modifier);
        groups
            .entry(labels_key(&labels))
            .or_default()
            .push((position, sample));
    }

    let mut out = Vec::new();
    for mut group in groups.into_values() {
        // Partitioning is unstable. The input position preserves the old stable
        // sort's choice and ordering even for identical labels and values (or
        // distinct NaN payloads), including ties across the selection boundary.
        let compare = |left: &(usize, InstantSample), right: &(usize, InstantSample)| {
            compare_k_aggregate_samples(op, &left.1, &right.1).then_with(|| left.0.cmp(&right.0))
        };
        if k < group.len() {
            group.select_nth_unstable_by(k, compare);
            group.truncate(k);
        }
        group.sort_unstable_by(compare);
        out.extend(group.into_iter().map(|(_, sample)| sample));
    }
    out
}

/// The `topk`/`bottomk` name that Prometheus spells in an annotation.
fn k_aggregate_name(op: TokenType) -> &'static str {
    if op.id() == T_TOPK {
        return "topk";
    }
    "bottomk"
}

#[cfg(test)]
mod tests;
