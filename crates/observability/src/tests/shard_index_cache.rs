use clap::Parser as _;

use crate::tests::prelude::{
    AclEntry, AclOperation, Arc, BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockKey,
    ColdBlockScan, CompactionFrontier, Duration, LOKI_REJECT_OLD_SAMPLES_MAX_AGE, LabelIndex,
    LogRow, LokiDirection, NonZeroUsize, ObjectPath, PatternType, PermissionType, QuerierState,
    QueryHotTail, RecordingObjectStore, ResourceType, SeriesParams, ServiceConfig,
    StreamScanOptions, TimeRange, check, connect_with_startup_retry, current_unix_time_ns, days,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
    execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options, hours,
    metadata_index_range, millis, minutes, parse_metric_query, parse_query, plan_stream_query,
    secs, write_log_block_to_object_store,
};

pub(crate) fn acl_entry(
    resource_type: ResourceType,
    resource_name: &str,
    pattern_type: PatternType,
    principal: &str,
    operation: AclOperation,
    permission_type: PermissionType,
) -> AclEntry {
    AclEntry {
        resource_type,
        resource_name: resource_name.to_string(),
        pattern_type,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type,
    }
}

/// Four one-row `app="api"` blocks for `tenant-a`, covering 0..=39 ns ten
/// nanoseconds apiece, in a store that delays every get by 25 ms.
pub(crate) struct FourColdApiBlocks {
    pub(crate) store: RecordingObjectStore,
    pub(crate) prefix: ObjectPath,
    pub(crate) label_index: LabelIndex,
    pub(crate) block_index: BlockIndex,
}

impl FourColdApiBlocks {
    pub(crate) async fn write() -> Self {
        let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
        let prefix = ObjectPath::from("observability/logs");
        let mut label_index = LabelIndex::default();
        let api = label_index.insert_series(
            "tenant-a",
            krabka_blockstore::labels([krabka_blockstore::LabelPair::new("app", "api")]),
        );
        let mut block_index = BlockIndex::default();
        for block_id in 0_i64..4 {
            let start_ns = block_id * 10;
            let end_ns = start_ns + 9;
            let block = write_log_block_to_object_store(
                &store,
                &prefix,
                &BlockKey::new(
                    "tenant-a",
                    0,
                    start_ns,
                    end_ns,
                    TimeRange::new(start_ns, end_ns).unwrap(),
                ),
                vec![LogRow::new(
                    api,
                    end_ns,
                    format!("api error {block_id}"),
                    BTreeMap::new(),
                )],
            )
            .await
            .unwrap();
            block_index.insert(block);
        }
        Self {
            store,
            prefix,
            label_index,
            block_index,
        }
    }
}

fn insert_api_series(
    labels_index: &mut LabelIndex,
    tenant: &str,
) -> krabka_blockstore::SeriesFingerprint {
    labels_index.insert_series(
        tenant,
        krabka_blockstore::labels([krabka_blockstore::LabelPair::new("app", "api")]),
    )
}

// --- FIX B3 tests ---

mod object_store_stream_query_batches_cold_block_reads;
mod querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    pub(crate) async fn querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range()
     {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_range = TimeRange::new(0, 100).unwrap();
        let mut labels_index = LabelIndex::default();
        let api = insert_api_series(&mut labels_index, tenant);
        let mut block_index = BlockIndex::default();
        let shard_range = TimeRange::new(10, 19).unwrap();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range),
            BTreeSet::from([api]),
        ));
        let state = querier_state_over_index_shards(
            &store,
            TenantIndexShards {
                prefix: &prefix,
                tenant,
                shard_ranges: &[shard_range],
                labels_index: &labels_index,
                block_index: &block_index,
            },
        )
        .await;

        let first = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert!(
            first.label_index.as_ref() == &labels_index
                && first.block_index.as_ref() == &block_index
        );
        assert!(
            second.label_index.as_ref() == &labels_index
                && second.block_index.as_ref() == &block_index
        );

        // A moving window misses the merged-request cache but still uses the same
        // tenant-filtered shard. It must retain the complete immutable snapshot.
        let moving = state
            .with_request_tenant_index(tenant, TimeRange::new(5, 105).unwrap())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.label_index, &moving.label_index));
        assert!(Arc::ptr_eq(&first.block_index, &moving.block_index));
        state.dynamic_index_cache.clear();
        assert!(moving.label_index.as_ref() == &labels_index);
        assert!(moving.block_index.as_ref() == &block_index);

        let list_count = shard_prefix_list_count(&store, &prefix, tenant);
        let shard_get_count = shard_snapshot_get_count(
            &store,
            TenantIndexShard {
                prefix: &prefix,
                tenant,
                shard_range,
            },
        );

        assert!(list_count == 1, "shard prefix should be listed once");
        assert!(
            shard_get_count == 1,
            "shard snapshot should be fetched once"
        );
    }
}
mod querier_state_with_request_tenant_index_lists_shards_from_query_window_offset {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    pub(crate) async fn querier_state_lists_full_shard_prefix_and_filters_before_fetch() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_start = 1_700_000_000_000_000_000;
        let query_end = query_start + 300_000_000_000;
        let query_range = TimeRange::new(query_start, query_end).unwrap();
        let old_shard_range =
            TimeRange::new(query_start - 600_000_000_000, query_start - 599_000_000_000).unwrap();
        let matching_shard_range = TimeRange::new(query_start + 10, query_start + 20).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = insert_api_series(&mut labels_index, tenant);
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 40, 41, old_shard_range),
            BTreeSet::from([api]),
        ));
        let matching_block = BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, matching_shard_range),
            BTreeSet::from([api]),
        );
        block_index.insert(matching_block.clone());
        let state = querier_state_over_index_shards(
            &store,
            TenantIndexShards {
                prefix: &prefix,
                tenant,
                shard_ranges: &[old_shard_range, matching_shard_range],
                labels_index: &labels_index,
                block_index: &block_index,
            },
        )
        .await;

        let state = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        let mut expected_blocks = BlockIndex::default();
        expected_blocks.insert(matching_block);
        assert!(
            state.label_index.as_ref() == &labels_index
                && state.block_index.as_ref() == &expected_blocks
        );

        let [old_shard_gets, matching_shard_gets] =
            [old_shard_range, matching_shard_range].map(|shard_range| {
                shard_snapshot_get_count(
                    &store,
                    TenantIndexShard {
                        prefix: &prefix,
                        tenant,
                        shard_range,
                    },
                )
            });
        assert!(old_shard_gets == 0);
        assert!(matching_shard_gets == 1);
        assert!(
            shard_prefix_list_count(&store, &prefix, tenant) == 1,
            "list the complete tenant prefix once"
        );
        assert!(
            store.list_offsets().is_empty(),
            "no offset can prove overlap coverage"
        );
    }
}
mod querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges {
    use krabka_units::prelude::TimeExt as _;

    use super::*;

    #[tokio::test]
    pub(crate) async fn querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges()
     {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let first_query_range = TimeRange::new(0, 100).unwrap();
        let moving_query_range = TimeRange::new(5, 105).unwrap();
        let shard_range_a = TimeRange::new(10, 19).unwrap();
        let shard_range_b = TimeRange::new(80, 89).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(
            tenant,
            krabka_blockstore::labels([krabka_blockstore::LabelPair::new("app", "api")]),
        );
        let worker = labels_index.insert_series(
            tenant,
            krabka_blockstore::labels([krabka_blockstore::LabelPair::new("app", "worker")]),
        );
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 44, 45, shard_range_b),
            BTreeSet::from([worker]),
        ));
        let state = querier_state_over_index_shards(
            &store,
            TenantIndexShards {
                prefix: &prefix,
                tenant,
                shard_ranges: &[shard_range_a, shard_range_b],
                labels_index: &labels_index,
                block_index: &block_index,
            },
        )
        .await;

        let first = state
            .with_request_tenant_index(tenant, first_query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, moving_query_range)
            .await
            .unwrap();

        for state in [&first, &second] {
            check!(
                state.label_index.as_ref() == &labels_index
                    && state.block_index.as_ref() == &block_index
            );
        }

        // A rolling query never repeats its exact cache key. Expired merged
        // snapshots must disappear when a later request admits a new window.
        for start in 6..10 {
            let expired = std::time::Instant::now()
                .checked_sub(secs(6).to_std())
                .unwrap();
            for entry in state
                .dynamic_index_cache
                .entries
                .lock()
                .unwrap()
                .values_mut()
            {
                entry.loaded_at = expired;
            }
            let moving = state
                .with_request_tenant_index(tenant, TimeRange::new(start, start + 100).unwrap())
                .await
                .unwrap();
            check!(moving.block_index.blocks().len() == 2);
            check!(
                moving.label_index.label_values(tenant, "app")
                    == BTreeSet::from(["api".to_string(), "worker".to_string()])
            );
            check!(state.dynamic_index_cache.entries.lock().unwrap().len() == 1);
        }
        check!(first.block_index.blocks().len() == 2);
        check!(
            first.label_index.label_values(tenant, "app")
                == BTreeSet::from(["api".to_string(), "worker".to_string()])
        );

        let list_count = shard_prefix_list_count(&store, &prefix, tenant);
        let [shard_get_count_a, shard_get_count_b] =
            [shard_range_a, shard_range_b].map(|shard_range| {
                shard_snapshot_get_count(
                    &store,
                    TenantIndexShard {
                        prefix: &prefix,
                        tenant,
                        shard_range,
                    },
                )
            });

        check!(list_count == 1, "shard prefix should be listed once");
        check!(
            shard_get_count_a == 1,
            "shard snapshot A should be fetched once"
        );
        check!(
            shard_get_count_b == 1,
            "shard snapshot B should be fetched once"
        );
    }
}
mod tenant_index_shards;

pub(crate) use tenant_index_shards::{
    TenantIndexShard, TenantIndexShards, querier_state_over_index_shards, shard_prefix_list_count,
    shard_snapshot_get_count,
};

#[test]
pub(crate) fn distributor_policy_uses_defaults_and_cli_overrides() {
    let defaults = ServiceConfig::parse_from(["krabka-observability", "--target", "distributor"]);
    check!(defaults.reject_old_samples_max_age == days(7));
    check!(defaults.creation_grace_period == minutes(10));
    check!(defaults.ingest_quota_burst_window == secs(1));
    check!(defaults.wal_connect_startup_deadline == minutes(2));
    check!(defaults.wal_connect_attempt_timeout == secs(15));
    check!(defaults.wal_connect_initial_backoff == millis(200));
    check!(defaults.wal_connect_max_backoff == secs(2));

    let configured = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--reject-old-samples-max-age=8d",
        "--creation-grace-period=11m",
        "--ingest-quota-burst-window=2s",
        "--wal-connect-startup-deadline=3m",
        "--wal-connect-attempt-timeout=16s",
        "--wal-connect-initial-backoff=300ms",
        "--wal-connect-max-backoff=3s",
    ])
    .expect("valid distributor policy");
    check!(configured.reject_old_samples_max_age == days(8));
    check!(configured.creation_grace_period == minutes(11));
    check!(configured.ingest_quota_burst_window == secs(2));
    check!(configured.wal_connect_startup_deadline == minutes(3));
    check!(configured.wal_connect_attempt_timeout == secs(16));
    check!(configured.wal_connect_initial_backoff == millis(300));
    check!(configured.wal_connect_max_backoff == secs(3));
}

#[test]
pub(crate) fn missing_timestamp_fallback_age_is_exact() {
    check!(LOKI_REJECT_OLD_SAMPLES_MAX_AGE == hours(168));
}

/// `connect_with_startup_retry` returns the error after the deadline is exceeded.
#[tokio::test]
pub(crate) async fn connect_with_startup_retry_gives_up_after_deadline() {
    let result: Result<u32, String> = connect_with_startup_retry(
        "test",
        millis(50), // very short deadline
        millis(10),
        millis(1),
        millis(10),
        || async { Err::<u32, String>("always fails".to_string()) },
    )
    .await;

    assert!(result.is_err(), "expected Err after deadline");
    assert_eq!(result.unwrap_err(), "always fails");
}

/// `connect_with_startup_retry` retries on failure and returns Ok when a later retry succeeds.
#[tokio::test]
pub(crate) async fn connect_with_startup_retry_retries_then_succeeds() {
    use std::sync::atomic::{AtomicU32, Ordering as AO};
    let counter = Arc::new(AtomicU32::new(0));
    let counter2 = counter.clone();

    let result: Result<u32, String> = connect_with_startup_retry(
        "test",
        secs(10),
        secs(1),
        millis(1),
        millis(10),
        move || {
            let c = counter2.clone();
            async move {
                let n = c.fetch_add(1, AO::SeqCst);
                if n < 2 {
                    Err(format!("not ready yet (attempt {n})"))
                } else {
                    Ok(99u32)
                }
            }
        },
    )
    .await;

    assert_eq!(result.unwrap(), 99);
    assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}

/// `connect_with_startup_retry` returns Ok immediately when the closure succeeds on the first try.
#[tokio::test]
pub(crate) async fn connect_with_startup_retry_succeeds_on_first_try() {
    let result: Result<u32, String> =
        connect_with_startup_retry("test", secs(5), secs(1), millis(1), millis(10), || async {
            Ok::<u32, String>(42)
        })
        .await;

    assert_eq!(result.unwrap(), 42);
}

#[test]
pub(crate) fn metadata_index_range_defaults_empty_metadata_requests_to_recent_window() {
    const SIX_HOURS_NS: i64 = 6 * 60 * 60 * 1_000_000_000;
    let before = current_unix_time_ns();
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default());
    let range = metadata_index_range(&state, &SeriesParams::default()).unwrap();
    let after = current_unix_time_ns();

    check!(
        range.start_ns >= before - SIX_HOURS_NS,
        "default metadata index start should be within Loki's default recent window"
    );
    check!(
        range.end_ns <= after,
        "default metadata index end should be now-ish, got {} after {}",
        range.end_ns,
        after
    );
    check!(
        range.end_ns - range.start_ns <= SIX_HOURS_NS,
        "default metadata index range should not be all time"
    );
}

/// Metric queries fetch their cold blocks the way stream queries do.
///
/// A `get_delay` of 25 ms per object is what makes the difference visible: one
/// round trip per block, serially, would never put two in flight at once, and
/// `max_active_gets` would stay at one however many blocks the query planned.
#[tokio::test]
pub(crate) async fn object_store_metric_query_batches_cold_block_reads() {
    let FourColdApiBlocks {
        store,
        prefix,
        label_index,
        block_index,
    } = FourColdApiBlocks::write().await;
    let tenant = "tenant-a";

    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [40ns])"#).unwrap();
    let plan = plan_stream_query(
        tenant,
        TimeRange::new(0, 39).unwrap(),
        query.stream.clone(),
        &label_index,
        &block_index,
    )
    .unwrap();
    check!(plan.blocks.len() == 4);

    let response = execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        ColdBlockScan {
            store: Arc::new(store.clone()),
            prefix: &prefix,
            block_fetch_concurrency: NonZeroUsize::new(4).unwrap(),
        },
        crate::querier::scan::MetricRangeScanRequest::new(
            &plan,
            &query,
            &label_index,
            crate::querier::scan::MetricEvaluationRange::new(TimeRange::new(39, 39).unwrap(), 1),
            QueryHotTail::<crate::WalLogRecord> {
                records: &[],
                frontier: &CompactionFrontier::new(i64::MAX),
                delete_filters: &[],
            },
        ),
    )
    .await
    .unwrap();

    check!(
        response
            .pointer("/data/result/0/values/0/1")
            .and_then(serde_json::Value::as_str)
            == Some("4")
    );
    check!(
        store.max_active_gets() > 1,
        "expected cold block reads to overlap, max_active_gets={}",
        store.max_active_gets()
    );
}
