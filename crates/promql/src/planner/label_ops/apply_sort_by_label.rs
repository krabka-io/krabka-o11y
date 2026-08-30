use super::{InstantSample, SortOrder, compare_label_values, labels_key};

/// Sorts an already-assembled instant vector by the values of the named labels
/// in `order`.
///
/// Ties break by canonical label key. This mirrors the interpreter's
/// `eval_sort_by_label_call`. The sort is over the listed labels first, in the
/// given order, and then over the full canonical label key. A `_desc` sort
/// therefore still tiebreaks by the ascending label key, exactly as the
/// interpreter's `labels_key` tiebreak does.
#[must_use]
pub fn apply_sort_by_label(
    mut samples: Vec<InstantSample>,
    label_names: &[String],
    order: SortOrder,
) -> Vec<InstantSample> {
    samples.sort_by(|left, right| {
        compare_label_values(&left.labels, &right.labels, label_names, order)
            .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
    });
    samples
}
