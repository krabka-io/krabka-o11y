use object_store::memory::InMemory;

use super::{
    super::prelude::{
        Arc, BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockKey, LabelIndex, ObjectPath,
        QuerierState, TimeRange, read_tenant_log_index_shards_from_object_store,
        write_tenant_log_index_shard_to_object_store,
    },
    *,
};

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
    let ranges = vec![super::super::prelude::TimeRange {
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

#[tokio::test]
async fn tenant_shard_range_cache_keeps_later_earlier_and_long_shards() {
    let cases = [
        (
            "unpadded timestamp listing order",
            vec![(100, 110), (115, 120)],
            vec![(100, 110, vec![0]), (105, 120, vec![0, 1])],
        ),
        (
            "signed timestamp listing order",
            vec![(-200, -100), (-150, -140)],
            vec![(-150, -140, vec![0, 1]), (-190, -180, vec![0])],
        ),
        (
            "later end",
            vec![(150, 160), (165, 170)],
            vec![(150, 160, vec![0]), (155, 170, vec![0, 1])],
        ),
        (
            "earlier start",
            vec![(300, 310), (500, 510)],
            vec![(500, 510, vec![1]), (300, 310, vec![0])],
        ),
        (
            "long shard starts before the listing offset",
            vec![(100, 900), (300, 310)],
            vec![(300, 310, vec![0, 1]), (700, 710, vec![0])],
        ),
    ];
    for (case, ranges, queries) in cases {
        let store = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("logs");
        let tenant = "tenant-a";
        let mut ledger = Vec::new();
        for (index, (start_ns, end_ns)) in ranges.into_iter().enumerate() {
            let range = TimeRange::new(start_ns, end_ns).unwrap();
            let labels = BTreeMap::from([("app".into(), format!("stream-{index}"))]);
            let mut label_index = LabelIndex::default();
            let fingerprint = label_index.insert_series(tenant, labels.clone());
            let offset = i64::try_from(index).unwrap();
            let descriptor = BlockDescriptor::new(
                BlockKey::new(tenant, 0, offset, offset, range),
                BTreeSet::from([fingerprint]),
            );
            let mut block_index = BlockIndex::default();
            block_index.insert(descriptor.clone());
            write_tenant_log_index_shard_to_object_store(
                store.as_ref(),
                &prefix,
                tenant,
                range,
                &label_index,
                &block_index,
            )
            .await
            .unwrap();
            ledger.push((descriptor, labels));
        }
        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_shards(store.clone(), prefix.clone());
        for (start_ns, end_ns, selected) in queries {
            let request = state
                .with_request_tenant_index(tenant, TimeRange::new(start_ns, end_ns).unwrap())
                .await
                .unwrap();
            let mut expected_labels = LabelIndex::default();
            let mut expected_blocks = BTreeMap::new();
            for index in selected {
                let (descriptor, labels) = &ledger[index];
                expected_labels.insert_series(tenant, labels.clone());
                expected_blocks.insert(descriptor.key.object_key(), descriptor.clone());
            }
            let actual_blocks = request
                .block_index
                .blocks()
                .iter()
                .map(|block| (block.key.object_key(), block.clone()))
                .collect::<BTreeMap<_, _>>();
            check!(
                request.label_index == expected_labels && actual_blocks == expected_blocks,
                "{case}: query [{start_ns}, {end_ns}]"
            );
            let (direct_labels, direct_blocks) = read_tenant_log_index_shards_from_object_store(
                store.as_ref(),
                &prefix,
                tenant,
                TimeRange::new(start_ns, end_ns).unwrap(),
            )
            .await
            .unwrap();
            let direct_blocks = direct_blocks
                .blocks()
                .iter()
                .map(|block| (block.key.object_key(), block.clone()))
                .collect::<BTreeMap<_, _>>();
            check!(
                direct_labels == expected_labels && direct_blocks == expected_blocks,
                "{case}: direct reader [{start_ns}, {end_ns}]"
            );
        }
    }
}
