use super::{Arc, GridVectors};

/// What the range driver's leaf memo can say about one leaf at one step.
pub(crate) enum LeafLookup {
    /// The leaf has not been planned yet. Plan it over the whole grid.
    Build,
    /// The grid path has given this leaf up for the rest of the query. Take the
    /// per-step path, and do not try to plan the grid again.
    Declined,
    /// Answer this step from the memoized whole-grid result.
    Ready(Arc<GridVectors>),
}
