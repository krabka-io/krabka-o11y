use super::{Labels, Ordering, SortOrder, natural_less};

/// Compares two label sets by the listed `label_names` in `order`.
///
/// This function returns the first non-equal label-value comparison, or
/// [`Ordering::Equal`] when every listed label is equal. A missing label
/// compares as the empty string.
///
/// Label values compare in NATURAL order, not byte order, because
/// `funcSortByLabel` compares them with `natsort`: `cpu="2"` therefore sorts
/// before `cpu="10"`. Two values that natural order calls neither equal nor
/// ascending compare as [`Ordering::Greater`], which is `funcSortByLabel`'s own
/// `return +1`.
pub(crate) fn compare_label_values(
    left: &Labels,
    right: &Labels,
    label_names: &[String],
    order: SortOrder,
) -> Ordering {
    for label_name in label_names {
        let left = left.get(label_name.as_str()).unwrap_or("");
        let right = right.get(label_name.as_str()).unwrap_or("");
        if left == right {
            continue;
        }
        let ordering = if natural_less(left, right) {
            Ordering::Less
        } else {
            Ordering::Greater
        };
        return match order {
            SortOrder::Ascending => ordering,
            SortOrder::Descending => ordering.reverse(),
        };
    }
    Ordering::Equal
}
