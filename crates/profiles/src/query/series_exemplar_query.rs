use super::{QueryRange, QueryTarget, Time};

/// A query for the exemplars of each series that `target` selects, grouped
/// by the `group_by` labels and bucketed by `step` over `range`.
#[derive(Clone, Copy)]
pub(crate) struct SeriesExemplarQuery<'a> {
    pub(crate) target: QueryTarget<'a>,
    pub(crate) group_by: &'a [String],
    pub(crate) step: Time,
    pub(crate) range: QueryRange,
    /// Keeps only stacks that match these call sites; empty keeps every stack.
    pub(crate) call_sites: &'a [String],
}
