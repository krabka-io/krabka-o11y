use object_store::memory::InMemory;

use super::{
    super::prelude::{
        Arc, BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockKey, LabelIndex, ObjectPath,
        QuerierState, TimeRange, read_tenant_log_index_shards_from_object_store,
        write_tenant_log_index_shard_to_object_store,
    },
    *,
};

struct QuerySelection {
    range: TimeRange,
    block_indices: Vec<usize>,
}

struct ShardRangeQueryCase {
    name: &'static str,
    ranges: Vec<TimeRange>,
    queries: Vec<QuerySelection>,
}

struct IndexedShardBlock {
    descriptor: BlockDescriptor,
    labels: BTreeMap<String, String>,
}

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
        ShardRangeQueryCase {
            name: "unpadded timestamp listing order",
            ranges: vec![
                TimeRange {
                    start_ns: 100,
                    end_ns: 110,
                },
                TimeRange {
                    start_ns: 115,
                    end_ns: 120,
                },
            ],
            queries: vec![
                QuerySelection {
                    range: TimeRange {
                        start_ns: 100,
                        end_ns: 110,
                    },
                    block_indices: vec![0],
                },
                QuerySelection {
                    range: TimeRange {
                        start_ns: 105,
                        end_ns: 120,
                    },
                    block_indices: vec![0, 1],
                },
            ],
        },
        ShardRangeQueryCase {
            name: "signed timestamp listing order",
            ranges: vec![
                TimeRange {
                    start_ns: -200,
                    end_ns: -100,
                },
                TimeRange {
                    start_ns: -150,
                    end_ns: -140,
                },
            ],
            queries: vec![
                QuerySelection {
                    range: TimeRange {
                        start_ns: -150,
                        end_ns: -140,
                    },
                    block_indices: vec![0, 1],
                },
                QuerySelection {
                    range: TimeRange {
                        start_ns: -190,
                        end_ns: -180,
                    },
                    block_indices: vec![0],
                },
            ],
        },
        ShardRangeQueryCase {
            name: "later end",
            ranges: vec![
                TimeRange {
                    start_ns: 150,
                    end_ns: 160,
                },
                TimeRange {
                    start_ns: 165,
                    end_ns: 170,
                },
            ],
            queries: vec![
                QuerySelection {
                    range: TimeRange {
                        start_ns: 150,
                        end_ns: 160,
                    },
                    block_indices: vec![0],
                },
                QuerySelection {
                    range: TimeRange {
                        start_ns: 155,
                        end_ns: 170,
                    },
                    block_indices: vec![0, 1],
                },
            ],
        },
        ShardRangeQueryCase {
            name: "earlier start",
            ranges: vec![
                TimeRange {
                    start_ns: 300,
                    end_ns: 310,
                },
                TimeRange {
                    start_ns: 500,
                    end_ns: 510,
                },
            ],
            queries: vec![
                QuerySelection {
                    range: TimeRange {
                        start_ns: 500,
                        end_ns: 510,
                    },
                    block_indices: vec![1],
                },
                QuerySelection {
                    range: TimeRange {
                        start_ns: 300,
                        end_ns: 310,
                    },
                    block_indices: vec![0],
                },
            ],
        },
        ShardRangeQueryCase {
            name: "long shard starts before the listing offset",
            ranges: vec![
                TimeRange {
                    start_ns: 100,
                    end_ns: 900,
                },
                TimeRange {
                    start_ns: 300,
                    end_ns: 310,
                },
            ],
            queries: vec![
                QuerySelection {
                    range: TimeRange {
                        start_ns: 300,
                        end_ns: 310,
                    },
                    block_indices: vec![0, 1],
                },
                QuerySelection {
                    range: TimeRange {
                        start_ns: 700,
                        end_ns: 710,
                    },
                    block_indices: vec![0],
                },
            ],
        },
    ];
    for case in cases {
        let store = Arc::new(InMemory::new());
        let prefix = ObjectPath::from("logs");
        let tenant = "tenant-a";
        let mut ledger: Vec<IndexedShardBlock> = Vec::new();
        for index in 0..case.ranges.len() {
            let range = case.ranges[index].clone();
            let mut labels = BTreeMap::new();
            labels.insert("app".into(), format!("stream-{index}"));
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
            ledger.push(IndexedShardBlock { descriptor, labels });
        }
        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_shards(store.clone(), prefix.clone());
        for query in case.queries {
            let start_ns = query.range.start_ns;
            let end_ns = query.range.end_ns;
            let request = state
                .with_request_tenant_index(tenant, query.range.clone())
                .await
                .unwrap();
            let mut expected_labels = LabelIndex::default();
            let mut expected_blocks = BTreeMap::new();
            for index in query.block_indices {
                let selected = &ledger[index];
                expected_labels.insert_series(tenant, selected.labels.clone());
                expected_blocks.insert(
                    selected.descriptor.key.object_key(),
                    selected.descriptor.clone(),
                );
            }
            let actual_blocks =
                request
                    .block_index
                    .blocks()
                    .iter()
                    .fold(BTreeMap::new(), |mut blocks, block| {
                        blocks.insert(block.key.object_key(), block.clone());
                        blocks
                    });
            check!(
                request.label_index.as_ref() == &expected_labels
                    && actual_blocks == expected_blocks,
                "{}: query [{start_ns}, {end_ns}]",
                case.name
            );
            let direct = read_tenant_log_index_shards_from_object_store(
                store.as_ref(),
                &prefix,
                tenant,
                query.range.clone(),
            )
            .await
            .unwrap();
            let direct_blocks =
                direct
                    .blocks
                    .blocks()
                    .iter()
                    .fold(BTreeMap::new(), |mut blocks, block| {
                        blocks.insert(block.key.object_key(), block.clone());
                        blocks
                    });
            check!(
                direct.labels == expected_labels && direct_blocks == expected_blocks,
                "{}: direct reader [{start_ns}, {end_ns}]",
                case.name
            );
        }
    }
}
