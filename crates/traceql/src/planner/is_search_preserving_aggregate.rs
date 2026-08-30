use super::*;

pub(crate) fn is_search_preserving_aggregate(agg: &Aggregate) -> bool {
    matches!(
        agg,
        Aggregate::Count
            | Aggregate::Sum(_)
            | Aggregate::Avg(_)
            | Aggregate::Min(_)
            | Aggregate::Max(_)
    )
}
