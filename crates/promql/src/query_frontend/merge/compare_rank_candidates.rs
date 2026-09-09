use std::cmp::Ordering;

use super::{RankCandidate, RankReduction};

/// Orders two rank candidates for a sharded `topk`/`bottomk` range query.
///
/// The first key is the sample value. `Top` puts the highest value first, and
/// `Bottom` puts the lowest value first. Both put a `NaN` last, because
/// Prometheus counts a `NaN` as neither a top value nor a bottom value.
///
/// `compare_k_aggregate_samples` applies the same rule on the unsharded path,
/// and the two keep one rule between them. A `topk` answered from shards and
/// the same `topk` answered whole would otherwise rank a `NaN` differently.
///
/// `total_cmp` alone does not give that result. It ranks a positive `NaN`
/// above every finite value, so `Top` would keep the `NaN` and drop a number.
/// So this function decides the `NaN` cases first, and `total_cmp` compares
/// two numbers only.
///
/// The remaining keys make the reduction deterministic whatever order the
/// shards return the candidates in. They are the canonical label key, then the
/// series position and the sample position of the candidate.
pub(crate) fn compare_rank_candidates(
    kind: RankReduction,
    left: &RankCandidate,
    right: &RankCandidate,
) -> Ordering {
    let by_value = match (left.value.is_nan(), right.value.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => match kind {
            RankReduction::Top => right.value.total_cmp(&left.value),
            RankReduction::Bottom => left.value.total_cmp(&right.value),
        },
    };
    by_value
        .then_with(|| left.labels_key.cmp(&right.labels_key))
        .then_with(|| left.series_index.cmp(&right.series_index))
        .then_with(|| left.sample_index.cmp(&right.sample_index))
}
