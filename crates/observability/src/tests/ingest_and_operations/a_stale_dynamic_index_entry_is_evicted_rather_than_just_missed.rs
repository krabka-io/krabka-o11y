use super::*;

/// The dynamic index caches hand back an entry only while it is fresh, and
/// EVICT a stale one on the way past rather than leaving it to be found
/// again. That eviction is the part worth pinning: a cache that returns
/// None but keeps the entry grows without bound for any key queried after
/// it expires.
///
/// A zero TTL reaches the stale branch without sleeping -- any elapsed
/// time at all is more than none. The boundary itself, an entry exactly at
/// its TTL, is not reachable against a real clock and is not attempted.
#[test]
pub(crate) fn a_stale_dynamic_index_entry_is_evicted_rather_than_just_missed() {
    let fresh = super::super::prelude::DynamicIndexCache {
        cache_ttl: krabka_units::hours(1),
        shard_cache_ttl: krabka_units::hours(1),
        ..super::super::prelude::DynamicIndexCache::default()
    };
    let stale = super::super::prelude::DynamicIndexCache {
        cache_ttl: Time::ZERO,
        shard_cache_ttl: Time::ZERO,
        ..super::super::prelude::DynamicIndexCache::default()
    };
    let key = || super::super::prelude::DynamicIndexCacheKey::TenantManifest {
        tenant: "tenant".to_string(),
    };
    let shard_key = || super::super::prelude::DynamicShardIndexCacheKey {
        tenant: "tenant".to_string(),
        start_ns: 0,
        end_ns: 10,
    };
    let held = |cache: &super::super::prelude::DynamicIndexCache| {
        (
            cache.entries.lock().expect("the cache lock is held").len(),
            cache
                .shard_indexes
                .lock()
                .expect("the shard cache lock is held")
                .len(),
        )
    };

    // Within the TTL: found, and still held afterwards.
    fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(fresh.get(&key()).is_some());
    check!(fresh.get_shard_index(&shard_key()).is_some());
    check!(
        held(&fresh) == (1, 1),
        "a fresh hit leaves the entry in place"
    );

    // Past the TTL: a miss, and the entry is gone rather than merely
    // ignored -- so a second lookup finds nothing to evict.
    stale.insert(key(), LabelIndex::default(), BlockIndex::default());
    stale.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(held(&stale) == (1, 1), "inserted");
    check!(stale.get(&key()).is_none());
    check!(stale.get_shard_index(&shard_key()).is_none());
    check!(held(&stale) == (0, 0), "and evicted on the way past");

    // A key that was never inserted is a miss without disturbing anything.
    check!(
        fresh
            .get(
                &super::super::prelude::DynamicIndexCacheKey::TenantManifest {
                    tenant: "other".to_string(),
                }
            )
            .is_none()
    );
    check!(held(&fresh) == (1, 1), "an absent key evicts nothing");

    // `clear` drops all three maps at once. It is what a configuration
    // reload calls, so with its body gone the querier keeps answering from
    // indexes built for the configuration it just replaced.
    fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_ranges(
        super::super::prelude::DynamicShardRangesCacheKey {
            tenant: "tenant".to_string(),
        },
        0,
        Vec::new(),
    );
    check!(
        fresh
            .shard_ranges
            .lock()
            .expect("the shard range lock is held")
            .len()
            == 1,
        "the third map is populated too"
    );
    fresh.clear();
    check!(held(&fresh) == (0, 0), "cleared");
    check!(
        fresh
            .shard_ranges
            .lock()
            .expect("the shard range lock is held")
            .is_empty(),
        "including the shard ranges"
    );
    check!(fresh.get(&key()).is_none(), "and a lookup misses");

    // The two caches have their OWN durations -- five seconds and five
    // minutes by default -- so each must read its own. With both set alike
    // a lookup consulting the wrong one behaves identically, so here they
    // are opposites: the manifest expires immediately and the shard index
    // does not, then the reverse.
    let short_manifest = super::super::prelude::DynamicIndexCache {
        cache_ttl: Time::ZERO,
        shard_cache_ttl: krabka_units::hours(1),
        ..super::super::prelude::DynamicIndexCache::default()
    };
    short_manifest.insert(key(), LabelIndex::default(), BlockIndex::default());
    short_manifest.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(
        short_manifest.get(&key()).is_none(),
        "the manifest ttl is zero"
    );
    check!(
        short_manifest.get_shard_index(&shard_key()).is_some(),
        "but the shard ttl is an hour"
    );

    let short_shard = super::super::prelude::DynamicIndexCache {
        cache_ttl: krabka_units::hours(1),
        shard_cache_ttl: Time::ZERO,
        ..super::super::prelude::DynamicIndexCache::default()
    };
    short_shard.insert(key(), LabelIndex::default(), BlockIndex::default());
    short_shard.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(
        short_shard.get(&key()).is_some(),
        "the manifest ttl is an hour"
    );
    check!(
        short_shard.get_shard_index(&shard_key()).is_none(),
        "but the shard ttl is zero"
    );
}
