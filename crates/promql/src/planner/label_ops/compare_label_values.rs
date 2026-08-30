use super::{Labels, SortOrder, Ordering};

/// Compares two label sets by the listed `label_names` in `order`.
///
/// This function returns the first non-equal label-value comparison, or
/// [`Ordering::Equal`] when every listed label is equal. A missing label
/// compares as the empty string. This mirrors the interpreter's
/// `SortDirection::compare_label_values`.
pub(crate) fn compare_label_values(
    left: &Labels,
    right: &Labels,
    label_names: &[String],
    order: SortOrder,
) -> Ordering {
    for label_name in label_names {
        let ordering = left
            .get(label_name.as_str())
            .unwrap_or("")
            .cmp(right.get(label_name.as_str()).unwrap_or(""));
        let ordering = match order {
            SortOrder::Ascending => ordering,
            SortOrder::Descending => ordering.reverse(),
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    Ordering::Equal
}
