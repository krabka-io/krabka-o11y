use super::{InstantSample, SortOrder, compare_label_values, labels_key};

/// Sorts an already-assembled instant vector by the values of the named labels
/// in `order`.
///
/// The sort is over the listed labels first, in the given order, and then over
/// the full canonical label key. The tiebreak follows `order` too:
/// `funcSortByLabelDesc` ends on `-labels.Compare(a, b)`, so two series that
/// agree on every listed label come back in DESCENDING label order.
#[must_use]
pub fn apply_sort_by_label(
    mut samples: Vec<InstantSample>,
    label_names: &[String],
    order: SortOrder,
) -> Vec<InstantSample> {
    samples.sort_by(|left, right| {
        compare_label_values(&left.labels, &right.labels, label_names, order).then_with(|| {
            let tiebreak = labels_key(&left.labels).cmp(&labels_key(&right.labels));
            match order {
                SortOrder::Ascending => tiebreak,
                SortOrder::Descending => tiebreak.reverse(),
            }
        })
    });
    samples
}
