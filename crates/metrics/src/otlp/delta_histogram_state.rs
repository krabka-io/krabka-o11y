use super::{Instant, NativeHistogram};

#[derive(Clone, Debug)]
pub(crate) struct DeltaHistogramState {
    pub(crate) start_time_unix_nano: u64,
    pub(crate) value: Option<NativeHistogram>,
    /// Monotonic instant of the last point folded into this stream. The
    /// accumulator drops the stream once it is stale.
    pub(crate) last_seen: Instant,
}

impl DeltaHistogramState {
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            start_time_unix_nano: 0,
            value: None,
            last_seen: now,
        }
    }
}
