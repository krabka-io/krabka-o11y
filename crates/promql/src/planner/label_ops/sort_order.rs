use super::Ordering;

/// Sort order for the `sort` / `sort_desc` functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    /// Compares two sample values in this order, and puts a `NaN` last in both.
    ///
    /// Prometheus sends a `NaN` to the bottom under `sort` and under
    /// `sort_desc`. `funcSort` and `funcSortDesc` each build a heap that ranks
    /// `NaN` first, then sort the reverse of that heap. A `NaN` is neither the
    /// largest value nor the smallest one, so no direction puts it first.
    ///
    /// `total_cmp` alone does not give that result. It ranks a positive `NaN`
    /// above every finite value. Ascending order then sends the `NaN` to the
    /// end, but descending order sends it to the front. So this method decides
    /// the `NaN` cases first, and `total_cmp` compares two numbers only.
    ///
    /// A histogram sample reaches this comparison as a `NaN` through
    /// `sort_value`, and so sorts last as well.
    pub(crate) fn compare(self, left: f64, right: f64) -> Ordering {
        match (left.is_nan(), right.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => match self {
                Self::Ascending => left.total_cmp(&right),
                Self::Descending => right.total_cmp(&left),
            },
        }
    }
}
