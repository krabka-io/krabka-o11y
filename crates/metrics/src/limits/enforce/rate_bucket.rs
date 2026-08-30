use super::{Arc, TokenBucket, AtomicU64};

/// Per-tenant ingestion-rate token bucket with a monotonic last-touch stamp.
/// The enforcer uses that stamp for least-recently-used eviction after the map
/// reaches `max_rate_buckets`.
#[derive(Debug)]
pub(crate) struct RateBucket {
    pub(crate) bucket: Arc<TokenBucket>,
    pub(crate) last_touch: AtomicU64,
}
