use super::*;

/// Sorts an already-assembled instant vector by sample value in `order`.
///
/// Ties break by canonical label key. This mirrors the interpreter's
/// `eval_sort_call`.
#[must_use]
pub fn apply_sort(mut samples: Vec<InstantSample>, order: SortOrder) -> Vec<InstantSample> {
    samples.sort_by(|left, right| {
        order
            .compare(sort_value(left), sort_value(right))
            .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
    });
    samples
}
