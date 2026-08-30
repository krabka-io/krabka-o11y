use super::*;

/// Sort order for the `sort` / `sort_desc` functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    /// Compares two sample values in this order with `total_cmp`.
    ///
    /// This matches the interpreter's `SortDirection::compare`. `total_cmp`
    /// places a positive `NaN` above every finite value, so ascending order
    /// sends `NaN` to the end. Descending order is the reverse and sends `NaN`
    /// to the front.
    pub(crate) fn compare(self, left: f64, right: f64) -> Ordering {
        match self {
            Self::Ascending => left.total_cmp(&right),
            Self::Descending => right.total_cmp(&left),
        }
    }
}
