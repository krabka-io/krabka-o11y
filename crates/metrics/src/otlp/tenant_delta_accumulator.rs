use super::{Arc, AtomicU64, DeltaAccumulator, Mutex};

/// One tenant's delta accumulator, with a monotonic last-touch stamp. The
/// holder uses that stamp for least-recently-used eviction once it reaches its
/// tenant cap.
#[derive(Debug)]
pub(crate) struct TenantDeltaAccumulator {
    pub(crate) accumulator: Arc<Mutex<DeltaAccumulator>>,
    pub(crate) last_touch: AtomicU64,
}
