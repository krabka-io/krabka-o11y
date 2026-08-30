use super::*;

/// The shard-range cache answers only when its entry is both fresh and
/// covers far enough back, and it *evicts* on either failure rather than
/// leaving the entry to be retried. Both halves matter: a caller that gets
/// None refetches, and an entry left behind would be rejected again on
/// every subsequent call while still occupying the map.
#[test]
pub(crate) fn a_stale_or_short_shard_range_entry_is_evicted_not_reused() {
    use std::time::{Duration, Instant};

    let key = super::super::prelude::DynamicShardRangesCacheKey {
        tenant: "t".to_string(),
    };
    let ranges = vec![super::prelude::TimeRange {
        start_ns: 100,
        end_ns: 200,
    }];

    let seed = |loaded_at: Instant, listed_from_ns: i64| {
        let cache = super::super::prelude::DynamicIndexCache::default();
        cache.shard_ranges.lock().expect("fresh lock").insert(
            key.clone(),
            super::super::prelude::CachedShardRanges {
                loaded_at,
                listed_from_ns,
                ranges: ranges.clone(),
            },
        );
        cache
    };
    let entries = |cache: &super::super::prelude::DynamicIndexCache| {
        cache.shard_ranges.lock().expect("fresh lock").len()
    };

    // Fresh, and covering back to 100: a request from 100 or later is served.
    let cache = seed(Instant::now(), 100);
    check!(
        cache.get_shard_ranges(&key, 100) == Some(ranges.clone()),
        "exactly covered"
    );
    check!(
        cache.get_shard_ranges(&key, 150) == Some(ranges.clone()),
        "more than covered"
    );
    check!(entries(&cache) == 1, "a usable entry stays");

    // Asked for earlier than the entry was listed from: not usable, and
    // dropped so the next call refetches rather than re-rejecting.
    let cache = seed(Instant::now(), 100);
    check!(
        cache.get_shard_ranges(&key, 99) == None,
        "one nanosecond short"
    );
    check!(entries(&cache) == 0, "and evicted");

    // Older than the five-second default TTL.
    let stale = Instant::now()
        .checked_sub(Duration::from_mins(1))
        .expect("an instant a minute ago");
    let cache = seed(stale, 100);
    check!(cache.get_shard_ranges(&key, 100) == None, "expired");
    check!(entries(&cache) == 0, "and evicted");

    // A key that was never cached is simply absent, and nothing is
    // inserted by asking for it.
    let cache = super::super::prelude::DynamicIndexCache::default();
    check!(cache.get_shard_ranges(&key, 100) == None);
    check!(entries(&cache) == 0);
}
