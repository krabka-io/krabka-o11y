use super::{Instant, MetricBlockStore, StdDurationExt, Time};

pub(crate) struct CachedMetricBlockStore {
    pub(crate) cached_at: Instant,
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) cold: MetricBlockStore,
}

impl CachedMetricBlockStore {
    pub(crate) fn covers(&self, start_ms: i64, end_ms: i64, ttl: Time) -> bool {
        self.cached_at.elapsed().as_time() < ttl
            && self.start_ms <= start_ms
            && self.end_ms >= end_ms
    }
}
