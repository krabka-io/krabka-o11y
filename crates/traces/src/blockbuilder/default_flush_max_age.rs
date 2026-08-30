use super::*;

/// Default maximum age of the oldest buffered span record before a flush.
///
/// In a cold-only deployment, such as the demo, there is no querier live tier.
/// The block-builder is then the only path that makes spans queryable, so this
/// age also bounds how stale recent-trace search and trace-by-id can be.
///
/// The value stays short, at 10s, so freshness stays close to the per-poll
/// behaviour. The `flush_max_records` cap still batches bursty traffic into
/// larger blocks, which is the proliferation case. A deployment that attaches a
/// querier live tier can raise this age to batch more aggressively at no
/// freshness cost.
pub const DEFAULT_FLUSH_MAX_AGE: Time = secs(10);
