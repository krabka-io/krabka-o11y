use super::{RankCandidate, RankReduction};

pub(crate) fn compare_rank_candidates(
    kind: RankReduction,
    left: &RankCandidate,
    right: &RankCandidate,
) -> std::cmp::Ordering {
    let by_value = match kind {
        RankReduction::Top => right.value.total_cmp(&left.value),
        RankReduction::Bottom => left.value.total_cmp(&right.value),
    };
    by_value
        .then_with(|| left.labels_key.cmp(&right.labels_key))
        .then_with(|| left.series_index.cmp(&right.series_index))
        .then_with(|| left.sample_index.cmp(&right.sample_index))
}
