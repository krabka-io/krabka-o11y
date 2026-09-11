use super::Instant;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DeltaState {
    pub(crate) start_time_unix_nano: u64,
    pub(crate) value: f64,
    /// Monotonic instant of the last point folded into this stream. The
    /// accumulator drops the stream once it is stale.
    pub(crate) last_seen: Instant,
}

impl DeltaState {
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            start_time_unix_nano: 0,
            value: 0.0,
            last_seen: now,
        }
    }
}
