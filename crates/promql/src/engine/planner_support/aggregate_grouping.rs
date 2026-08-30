use super::{LabelModifier, Grouping};

/// Maps an aggregation `by`/`without` modifier to the planner [`Grouping`].
///
/// `None` means the aggregation has no modifier. The caller treats that as
/// `by ()`, which collapses all series into one group.
pub(crate) fn aggregate_grouping(modifier: Option<&LabelModifier>) -> Option<Grouping> {
    match modifier? {
        LabelModifier::Include(include) => Some(Grouping::By(include.labels.clone())),
        LabelModifier::Exclude(exclude) => Some(Grouping::Without(exclude.labels.clone())),
    }
}
