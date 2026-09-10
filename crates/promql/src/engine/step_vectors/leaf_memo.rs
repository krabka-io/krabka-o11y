use super::{Arc, GridVectors};

/// What the range driver's leaf memo holds for one leaf it has been asked for.
#[derive(Clone)]
pub(crate) enum LeafMemo {
    /// The grid path has given this leaf up for the rest of the query, because
    /// it would take more than the memo's size budget or because its scalar
    /// parameter turned out to vary between steps. Recorded so the step loop
    /// asks once rather than at every step.
    Declined,
    /// The leaf's whole-grid result, and the scalar parameter it was built with.
    ///
    /// `parameter` is `quantile_over_time`'s `phi`, whose bit pattern is exact
    /// enough to distinguish two quantiles and to treat NaN as one value. It is
    /// resolved per step, so a leaf whose parameter is itself an expression can
    /// evaluate to a different quantile at a different step: the memo is only
    /// valid for the value it was built with. Every other leaf has no scalar
    /// parameter and passes zero.
    Ready {
        parameter: u64,
        vectors: Arc<GridVectors>,
    },
}
