use super::{Retained, VecDeque};

/// Retained source records plus the count of records evicted since the last
/// rebuild. The store uses that count to amortize rebuilds. See
/// [`REBUILD_AMORTIZE_FACTOR`].
#[derive(Default)]
pub(crate) struct RetainedState {
    pub(crate) records: VecDeque<Retained>,
    pub(crate) evicted_since_rebuild: usize,
}
