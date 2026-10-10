use super::Ordering;

/// The order a result's samples are ranked in, by value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SampleOrder {
    /// Smallest first: `sort`, and the samples `bottomk` keeps.
    Ascending,
    /// Largest first: `sort_desc`, and the samples `topk` keeps.
    Descending,
}

impl SampleOrder {
    /// Orients `ascending`, an ascending comparison, to this order.
    pub(crate) fn orient(self, ascending: Ordering) -> Ordering {
        match self {
            Self::Ascending => ascending,
            Self::Descending => ascending.reverse(),
        }
    }
}
