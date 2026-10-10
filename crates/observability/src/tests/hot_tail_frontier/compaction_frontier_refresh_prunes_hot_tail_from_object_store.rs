use assert2::assert;
use krabka_blockstore::{
    LogRow, labels, log_tenant_index_shard_manifest_object_path,
    read_tenant_log_index_shard_from_object_store, update_tenant_log_index_shard_to_object_store,
    write_tenant_log_index_shard_to_object_store,
};
use object_store::ObjectStoreExt as _;

use super::*;
use crate::{
    HttpStreamQuery, LokiDirection, LokiStreamEncoding, LokiStreamOptions, ServiceConfig,
    compact_log_block_to_object_store_with_index_output, execute_http_stream_query,
};

#[tokio::test]
pub(crate) async fn compaction_frontier_refresh_prunes_hot_tail_from_object_store() {
    let store = object_store::memory::InMemory::new();
    let prefix = ObjectPath::default();
    let frontier = SharedCompactionFrontier::default();
    let hot_tail = BufferedLogHotTail::default();
    let compacted = hot_tail_test_record(1_000, "old");
    let fresh = hot_tail_test_record(3_000, "new");
    hot_tail.append_records(vec![compacted, fresh.clone()]);
    write_compaction_frontier_to_object_store(&store, &prefix, &CompactionFrontier::new(2_000))
        .await
        .unwrap();

    let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
        .await
        .unwrap();

    assert_eq!(pruned, 1);
    assert_eq!(frontier.snapshot(), CompactionFrontier::new(2_000));
    assert_eq!(hot_tail.records(), vec![fresh]);
}

#[tokio::test]
async fn refreshed_frontier_keeps_newly_published_rows_visible_with_a_cached_index() {
    assert_rows_survive_handoff(false).await;
}

#[tokio::test]
async fn an_old_blocked_index_load_cannot_refill_the_new_frontier_cache() {
    assert_rows_survive_handoff(true).await;
}

#[tokio::test]
async fn different_wal_blocks_with_equal_time_ranges_survive_frontier_pruning() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let prefix = ObjectPath::from("logs");
    let range = TimeRange::new(19, 20).unwrap();
    let hot_tail = BufferedLogHotTail::default();
    let frontier = SharedCompactionFrontier::default();
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    for (app, first_offset) in [("api", 1), ("worker", 3)] {
        let source = labels([("app", app)]);
        let fingerprint = label_index.insert_series("tenant-a", source.clone());
        let mut rows = Vec::new();
        for (timestamp, offset) in [(19, first_offset), (20, first_offset + 1)] {
            let line = format!("{app}-{timestamp}");
            rows.push(LogRow::new(
                fingerprint,
                timestamp,
                line.clone(),
                BTreeMap::new(),
            ));
            hot_tail.append_records(vec![WalLogRecord {
                tenant: "tenant-a".into(),
                labels: source.clone(),
                timestamp_ns: timestamp,
                line,
                structured_metadata: BTreeMap::new(),
                position: Some(WalPosition {
                    partition: PartitionIndex(0),
                    offset: Offset(offset),
                }),
            }]);
        }
        compact_log_block_to_object_store_with_index_output(
            store.as_ref(),
            &prefix,
            &BlockKey::new("tenant-a", 0, first_offset, first_offset + 1, range),
            &label_index,
            &mut block_index,
            rows,
            LogCompactionIndexOutput::ShardManifests,
        )
        .await
        .unwrap();
    }
    assert!(block_index.blocks().len() == 2);
    write_compaction_frontier_to_object_store(
        store.as_ref(),
        &prefix,
        &CompactionFrontier::new(i64::MIN).with_partition_offset(PartitionIndex(0), Offset(4)),
    )
    .await
    .unwrap();
    assert!(
        refresh_compaction_frontier_and_prune(store.as_ref(), &prefix, &frontier, &hot_tail)
            .await
            .unwrap()
            == 4
    );
    assert!(hot_tail.records().is_empty());
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_cold_object_store_source(store.clone(), prefix.clone())
        .with_dynamic_tenant_object_store_shards(store, prefix)
        .with_hot_tail_shared_frontier(hot_tail, frontier);
    let mut response = execute_http_stream_query(
        &state,
        HttpStreamQuery {
            query: r#"{app=~"api|worker"}"#,
            tenant: "tenant-a",
            time_range: TimeRange::new(0, 30).unwrap(),
            options: LokiStreamOptions {
                direction: LokiDirection::Forward,
                limit: Some(10),
                interval: None,
            },
            end_exclusive: Some(30),
            encoding: LokiStreamEncoding::Folded,
        },
    )
    .await
    .unwrap();
    response["data"].as_object_mut().unwrap().remove("stats");
    assert!(
        response
            == json!({
                "status": "success", "data": {"resultType": "streams", "result": [
                    {"stream": {"app": "api"}, "values": [["19", "api-19"], ["20", "api-20"]]},
                    {"stream": {"app": "worker"}, "values": [["19", "worker-19"], ["20", "worker-20"]]}
                ]}
            })
    );
}

#[tokio::test]
async fn metric_frontend_reads_cold_and_hot_rows_before_the_evaluation_range() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let prefix = ObjectPath::from("logs");
    let mut label_index = LabelIndex::default();
    let source = labels([("app", "api")]);
    let fingerprint = label_index.insert_series("tenant-a", source.clone());
    compact_log_block_to_object_store_with_index_output(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 1, 1, TimeRange::new(19, 19).unwrap()),
        &label_index,
        &mut BlockIndex::default(),
        vec![LogRow::new(fingerprint, 19, "cold", BTreeMap::new())],
        LogCompactionIndexOutput::ShardManifests,
    )
    .await
    .unwrap();
    let hot_tail = BufferedLogHotTail::default();
    hot_tail.append_records(vec![WalLogRecord {
        tenant: "tenant-a".into(),
        labels: source,
        timestamp_ns: 20,
        line: "hot".into(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }]);
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_cold_object_store_source(store.clone(), prefix.clone())
        .with_dynamic_tenant_object_store_shards(store, prefix)
        .with_hot_tail_shared_frontier(hot_tail, SharedCompactionFrontier::default());
    for (query, at, want) in [
        (r#"count_over_time({app="api"}[15ns])"#, 30, "2"),
        (r#"count_over_time({app="api"}[15ns] offset 10ns)"#, 40, "2"),
        (
            r#"count_over_time({app="api"}[25ns]) + count_over_time({app="api"}[20ns] offset 10ns)"#,
            40,
            "4",
        ),
    ] {
        let encoded: String = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("start", &format!("0.{at:09}"))
            .append_pair("end", &format!("0.{at:09}"))
            .append_pair("step", "1ns")
            .finish();
        let params = crate::parse_query_params(Some(&encoded)).unwrap();
        let mut response = crate::execute_logs_query_frontend(
            &state,
            &krabka_blockstore::TenantId::new("tenant-a").unwrap(),
            &params,
            LokiStreamEncoding::Folded,
        )
        .await
        .unwrap();
        response["data"].as_object_mut().unwrap().remove("stats");
        let timestamp: f64 = format!("0.{at:09}").parse().unwrap();
        assert!(
            response
                == json!({
                    "status": "success", "data": {"resultType": "matrix", "result": [
                        {"metric": {"app": "api"}, "values": [[timestamp, want]]}
                    ]}
                }),
            "{query}"
        );
    }
}

#[tokio::test]
async fn metadata_without_time_bounds_keeps_hot_labels_outside_the_cold_window() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt as _;

    let store = Arc::new(object_store::memory::InMemory::new());
    let hot_tail = BufferedLogHotTail::default();
    hot_tail.append_records(vec![WalLogRecord {
        tenant: "tenant-a".into(),
        labels: labels([("app", "api")]),
        timestamp_ns: 20,
        line: "hot".into(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }]);
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_cold_object_store_source(store.clone(), ObjectPath::from("logs"))
        .with_dynamic_tenant_object_store_shards(store, ObjectPath::from("logs"))
        .with_hot_tail_shared_frontier(hot_tail, SharedCompactionFrontier::default());
    let app = crate::loki_router(state);
    for (uri, data) in [
        ("/loki/api/v1/labels", json!(["app"])),
        ("/loki/api/v1/label/app/values", json!(["api"])),
        (
            "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D",
            json!([{"app":"api"}]),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status() == axum::http::StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value == json!({"status": "success", "data": data}), "{uri}");
    }
}

async fn stream_response(state: &QuerierState) -> serde_json::Value {
    let mut response = execute_http_stream_query(
        state,
        HttpStreamQuery {
            query: r#"{app="api"}"#,
            tenant: "tenant-a",
            time_range: TimeRange::new(0, 30).unwrap(),
            options: LokiStreamOptions {
                direction: LokiDirection::Forward,
                limit: Some(10),
                interval: None,
            },
            end_exclusive: Some(30),
            encoding: LokiStreamEncoding::Folded,
        },
    )
    .await
    .unwrap();
    response["data"].as_object_mut().unwrap().remove("stats");
    response
}

async fn assert_rows_survive_handoff(blocked_load: bool) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("logs");
    let store = Arc::new(store);
    let mut label_index = LabelIndex::default();
    let source = labels([("app", "api")]);
    let fingerprint = label_index.insert_series("tenant-a", source.clone());
    let mut block_index = BlockIndex::default();
    let first = compact_log_block_to_object_store_with_index_output(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 1, 1, TimeRange::new(19, 19).unwrap()),
        &label_index,
        &mut block_index,
        vec![LogRow::new(fingerprint, 19, "first", BTreeMap::new())],
        LogCompactionIndexOutput::ShardManifests,
    )
    .await
    .unwrap();
    let store = Arc::new(if blocked_load {
        (*store).clone().with_get_barrier(
            shard_snapshot_path(&prefix, TimeRange::new(19, 19).unwrap(), 0).to_string(),
            entered.clone(),
            release.clone(),
        )
    } else {
        (*store).clone()
    });
    let frontier = SharedCompactionFrontier::new(
        CompactionFrontier::new(i64::MIN).with_partition_offset(PartitionIndex(0), Offset(1)),
    );
    let hot_tail = BufferedLogHotTail::default();
    hot_tail.append_records(vec![WalLogRecord {
        tenant: "tenant-a".into(),
        labels: source,
        timestamp_ns: 20,
        line: "second".into(),
        structured_metadata: BTreeMap::new(),
        position: Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(2),
        }),
    }]);
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_runtime_policy(&ServiceConfig {
            querier_dynamic_index_cache_ttl: krabka_units::hours(1),
            ..ServiceConfig::default()
        })
        .with_cold_object_store_source(store.clone(), prefix.clone())
        .with_dynamic_tenant_object_store_shards(store.clone(), prefix.clone())
        .with_hot_tail_shared_frontier(hot_tail.clone(), frontier.clone());
    let expected = json!({
        "status": "success", "data": {"resultType": "streams", "result": [
            {"stream": {"app": "api"}, "values": [["19", "first"], ["20", "second"]]}
        ]}
    });
    let pending = if blocked_load {
        let state = state.clone();
        let query = tokio::spawn(async move { stream_response(&state).await });
        entered.notified().await;
        Some(query)
    } else {
        assert!(stream_response(&state).await == expected);
        None
    };

    // Publish both the physical block and its shard before advancing the frontier.
    let second = compact_log_block_to_object_store_with_index_output(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 2, 2, TimeRange::new(20, 20).unwrap()),
        &label_index,
        &mut block_index,
        vec![LogRow::new(fingerprint, 20, "second", BTreeMap::new())],
        LogCompactionIndexOutput::ShardManifests,
    )
    .await
    .unwrap();
    assert!(first.key != second.key);
    write_compaction_frontier_to_object_store(
        store.as_ref(),
        &prefix,
        &CompactionFrontier::new(i64::MIN).with_partition_offset(PartitionIndex(0), Offset(2)),
    )
    .await
    .unwrap();
    assert!(
        refresh_compaction_frontier_and_prune(store.as_ref(), &prefix, &frontier, &hot_tail,)
            .await
            .unwrap()
            == 1
    );
    assert!(hot_tail.records().is_empty());
    assert!(stream_response(&state).await == expected);
    if let Some(pending) = pending {
        release.add_permits(1);
        assert!(pending.await.unwrap() == expected);
        assert!(stream_response(&state).await == expected);
    }
}

fn shard_snapshot_path(prefix: &ObjectPath, range: TimeRange, generation: u64) -> ObjectPath {
    let key = log_tenant_index_shard_manifest_object_path(prefix, "tenant-a", range);
    ObjectPath::from(krabka_blockstore::index_snapshot_prefix_for_key(
        key.as_ref(),
    ))
    .join(format!("{generation:020}.json"))
}

fn shard_indexes(app: &str, offset: i64) -> (LabelIndex, BlockIndex) {
    let mut labels = LabelIndex::default();
    let fingerprint = labels.insert_series("tenant-a", krabka_blockstore::labels([("app", app)]));
    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new(
            "tenant-a",
            0,
            offset,
            offset,
            TimeRange::new(19, 20).unwrap(),
        ),
        BTreeSet::from([fingerprint]),
    ));
    (labels, blocks)
}

/// A tenant shard store whose first read of generation 0 parks until
/// `release` gets a permit, after signalling `entered`.
struct BarrieredShard {
    prefix: ObjectPath,
    range: TimeRange,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    store: Arc<RecordingObjectStore>,
}

impl Default for BarrieredShard {
    fn default() -> Self {
        let prefix = ObjectPath::from("logs");
        let range = TimeRange::new(19, 20).unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let store = Arc::new(RecordingObjectStore::new().with_get_barrier(
            shard_snapshot_path(&prefix, range, 0).to_string(),
            entered.clone(),
            release.clone(),
        ));
        Self {
            prefix,
            range,
            entered,
            release,
            store,
        }
    }
}

/// Writes the first generation of the [`BarrieredShard`] tenant shard.
async fn seed_shard(store: &RecordingObjectStore, labels: &LabelIndex, blocks: &BlockIndex) {
    write_tenant_log_index_shard_to_object_store(
        store,
        &ObjectPath::from("logs"),
        "tenant-a",
        TimeRange::new(19, 20).unwrap(),
        labels,
        blocks,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn shard_deltas_retry_conflicts_and_preserve_concurrent_append_labels() {
    for change in ["append", "remove", "replace"] {
        let BarrieredShard {
            prefix,
            range,
            entered,
            release,
            store,
        } = BarrieredShard::default();
        let (original_labels, original_blocks) = shard_indexes("original", 1);
        seed_shard(store.as_ref(), &original_labels, &original_blocks).await;
        store.clear_put_paths();
        let previous = if change == "append" {
            BlockIndex::default()
        } else {
            original_blocks
        };
        let (next_labels, next_blocks) = match change {
            "append" => shard_indexes("first-append", 3),
            "remove" => (LabelIndex::default(), BlockIndex::default()),
            _ => shard_indexes("replacement", 1),
        };
        let pending = {
            let store = store.clone();
            let prefix = prefix.clone();
            tokio::spawn(async move {
                update_tenant_log_index_shard_to_object_store(
                    store.as_ref(),
                    &prefix,
                    "tenant-a",
                    range,
                    &previous,
                    &next_labels,
                    &next_blocks,
                )
                .await
            })
        };
        entered.notified().await;
        let (mut expected_labels, mut expected_blocks) = shard_indexes("concurrent", 2);
        update_tenant_log_index_shard_to_object_store(
            store.as_ref(),
            &prefix,
            "tenant-a",
            range,
            &BlockIndex::default(),
            &expected_labels,
            &expected_blocks,
        )
        .await
        .unwrap();
        release.add_permits(1);
        pending.await.unwrap().unwrap();
        // The losing conditional put must retry; exactly two writes would mean
        // the interleaving never exercised the conflict.
        assert!(store.put_paths().len() == 3, "{change}");
        for (app, offset) in match change {
            "append" => vec![("original", 1), ("first-append", 3)],
            "remove" => vec![],
            _ => vec![("replacement", 1)],
        } {
            let (extra_labels, extra_blocks) = shard_indexes(app, offset);
            for (_, labels) in extra_labels.tenant_series("tenant-a") {
                expected_labels.insert_series("tenant-a", labels);
            }
            for block in extra_blocks.blocks() {
                expected_blocks.insert(block.clone());
            }
        }
        assert!(
            read_tenant_log_index_shard_from_object_store(
                store.as_ref(),
                &prefix,
                "tenant-a",
                range,
            )
            .await
            .unwrap()
                == (expected_labels, expected_blocks),
            "{change}"
        );
    }
}

#[tokio::test]
async fn stale_retention_keeps_an_append_to_an_otherwise_empty_shard() {
    let BarrieredShard {
        prefix,
        range,
        entered,
        release,
        store,
    } = BarrieredShard::default();
    let (old_labels, old_blocks) = shard_indexes("expired", 1);
    seed_shard(store.as_ref(), &old_labels, &old_blocks).await;
    let pending = {
        let store = store.clone();
        let prefix = prefix.clone();
        tokio::spawn(async move {
            let windows =
                crate::OverridesProvider::from_yaml("defaults:\n  retention_period: \"1ns\"\n")
                    .unwrap();
            crate::compactor::retention::sweep_expired_tenant_log_blocks(
                store.as_ref(),
                &prefix,
                "tenant-a",
                100,
                &windows,
            )
            .await
        })
    };
    entered.notified().await;
    let (new_labels, new_blocks) = shard_indexes("appended", 2);
    update_tenant_log_index_shard_to_object_store(
        store.as_ref(),
        &prefix,
        "tenant-a",
        range,
        &BlockIndex::default(),
        &new_labels,
        &new_blocks,
    )
    .await
    .unwrap();
    release.add_permits(1);
    let deletions = pending.await.unwrap().unwrap();
    assert!(
        deletions
            == vec![krabka_blockstore::BlockDeletion {
                object_key: krabka_blockstore::log_block_object_path(
                    &prefix,
                    &old_blocks.blocks()[0].key,
                )
                .to_string(),
                sidecars: vec![],
            }]
    );
    assert!(
        read_tenant_log_index_shard_from_object_store(store.as_ref(), &prefix, "tenant-a", range,)
            .await
            .unwrap()
            == (new_labels, new_blocks)
    );
}

#[tokio::test]
async fn a_blocked_replay_append_does_not_resurrect_a_removed_descriptor() {
    let BarrieredShard {
        prefix,
        range,
        entered,
        release,
        store,
    } = BarrieredShard::default();
    let (labels, blocks) = shard_indexes("original", 1);
    seed_shard(store.as_ref(), &labels, &blocks).await;
    store.clear_put_paths();
    let replay = {
        let store = store.clone();
        let prefix = prefix.clone();
        let labels = labels.clone();
        let blocks = blocks.clone();
        tokio::spawn(async move {
            update_tenant_log_index_shard_to_object_store(
                store.as_ref(),
                &prefix,
                "tenant-a",
                range,
                &BlockIndex::default(),
                &labels,
                &blocks,
            )
            .await
        })
    };
    entered.notified().await;
    update_tenant_log_index_shard_to_object_store(
        store.as_ref(),
        &prefix,
        "tenant-a",
        range,
        &blocks,
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .await
    .unwrap();
    release.add_permits(1);
    replay.await.unwrap().unwrap();
    assert!(store.put_paths().len() == 3);
    assert!(
        read_tenant_log_index_shard_from_object_store(store.as_ref(), &prefix, "tenant-a", range)
            .await
            .unwrap()
            == (LabelIndex::default(), BlockIndex::default())
    );
}

#[tokio::test]
async fn shard_deltas_are_idempotent_and_do_not_resurrect_stale_descriptors() {
    let store = object_store::memory::InMemory::new();
    let prefix = ObjectPath::from("logs");
    let range = TimeRange::new(19, 20).unwrap();
    let (old_labels, old_blocks) = shard_indexes("old", 1);
    let (new_labels, new_blocks) = shard_indexes("new", 1);
    write_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        range,
        &new_labels,
        &new_blocks,
    )
    .await
    .unwrap();
    for previous in [BlockIndex::default(), old_blocks.clone()] {
        update_tenant_log_index_shard_to_object_store(
            &store,
            &prefix,
            "tenant-a",
            range,
            &previous,
            &old_labels,
            &old_blocks,
        )
        .await
        .unwrap();
        assert!(
            read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", range,)
                .await
                .unwrap()
                == (new_labels.clone(), new_blocks.clone())
        );
    }
    update_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        range,
        &old_blocks,
        &new_labels,
        &new_blocks,
    )
    .await
    .unwrap();
    let error = update_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        range,
        &old_blocks,
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        krabka_blockstore::LogBlockStoreError::ObjectStore(
            object_store::Error::Precondition { .. }
        )
    ));
    update_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        range,
        &new_blocks,
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .await
    .unwrap();
    update_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        range,
        &old_blocks,
        &old_labels,
        &old_blocks,
    )
    .await
    .unwrap();
    assert!(
        read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", range,)
            .await
            .unwrap()
            == (LabelIndex::default(), BlockIndex::default())
    );
}

#[tokio::test]
async fn invalid_shard_manifests_fail_before_conditional_publication() {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("logs");
    let range = TimeRange::new(19, 20).unwrap();
    let path = shard_snapshot_path(&prefix, range, 0);
    let (labels, blocks) = shard_indexes("api", 1);
    for malformed in [
        r#"{"format_version":4294967295,"series":[],"blocks":[]}"#,
        r#"{"series":[],"blocks":[]}"#,
        r#"{"format_version":1,"series":[{"tenant":"tenant-a","fingerprint":0,"labels":{"app":"api"}}],"blocks":[]}"#,
        "not JSON",
    ] {
        store
            .put(&path, malformed.to_string().into())
            .await
            .unwrap();
        store.clear_put_paths();
        assert!(
            update_tenant_log_index_shard_to_object_store(
                &store,
                &prefix,
                "tenant-a",
                range,
                &BlockIndex::default(),
                &labels,
                &blocks,
            )
            .await
            .is_err()
        );
        assert!(store.put_paths().is_empty());
        assert!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref()
                == malformed.as_bytes()
        );
    }
}

#[tokio::test]
async fn a_paused_shard_writer_retries_a_reclaimed_generation_instead_of_losing_its_append() {
    let BarrieredShard {
        prefix,
        range,
        entered,
        release,
        store,
    } = BarrieredShard::default();
    let (mut expected_labels, mut expected_blocks) = shard_indexes("initial", 0);
    seed_shard(store.as_ref(), &expected_labels, &expected_blocks).await;
    let pending = {
        let store = store.clone();
        let prefix = prefix.clone();
        tokio::spawn(async move {
            let (labels, blocks) = shard_indexes("paused", 100);
            update_tenant_log_index_shard_to_object_store(
                store.as_ref(),
                &prefix,
                "tenant-a",
                range,
                &BlockIndex::default(),
                &labels,
                &blocks,
            )
            .await
        })
    };
    entered.notified().await;
    // More than the retained eight generations reclaim the paused writer's
    // proposed generation1. Its first Create can succeed, but is obsolete.
    for offset in 1..=12 {
        let (labels, blocks) = shard_indexes(&format!("append-{offset}"), offset);
        update_tenant_log_index_shard_to_object_store(
            store.as_ref(),
            &prefix,
            "tenant-a",
            range,
            &BlockIndex::default(),
            &labels,
            &blocks,
        )
        .await
        .unwrap();
        for (_, series) in labels.tenant_series("tenant-a") {
            expected_labels.insert_series("tenant-a", series);
        }
        expected_blocks.insert(blocks.blocks()[0].clone());
    }
    assert!(matches!(
        store
            .inner
            .get(&shard_snapshot_path(&prefix, range, 1))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    release.add_permits(1);
    pending.await.unwrap().unwrap();
    let (paused_labels, paused_blocks) = shard_indexes("paused", 100);
    for (_, series) in paused_labels.tenant_series("tenant-a") {
        expected_labels.insert_series("tenant-a", series);
    }
    expected_blocks.insert(paused_blocks.blocks()[0].clone());
    assert!(
        read_tenant_log_index_shard_from_object_store(store.as_ref(), &prefix, "tenant-a", range,)
            .await
            .unwrap()
            == (expected_labels, expected_blocks)
    );
}

#[tokio::test]
async fn a_shard_reader_reselects_the_latest_generation_when_its_selected_object_is_pruned() {
    let prefix = ObjectPath::from("logs");
    let range = TimeRange::new(19, 20).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let store = Arc::new(RecordingObjectStore::new().with_get_barrier_before_read(
        shard_snapshot_path(&prefix, range, 0).to_string(),
        entered.clone(),
        release.clone(),
    ));
    let (labels, blocks) = shard_indexes("old", 0);
    write_tenant_log_index_shard_to_object_store(
        store.as_ref(),
        &prefix,
        "tenant-a",
        range,
        &labels,
        &blocks,
    )
    .await
    .unwrap();
    let pending = {
        let store = store.clone();
        let prefix = prefix.clone();
        tokio::spawn(async move {
            read_tenant_log_index_shard_from_object_store(
                store.as_ref(),
                &prefix,
                "tenant-a",
                range,
            )
            .await
        })
    };
    entered.notified().await;
    for offset in 1..=12 {
        let (labels, blocks) = shard_indexes("latest", offset);
        write_tenant_log_index_shard_to_object_store(
            store.as_ref(),
            &prefix,
            "tenant-a",
            range,
            &labels,
            &blocks,
        )
        .await
        .unwrap();
    }
    assert!(matches!(
        store
            .inner
            .get(&shard_snapshot_path(&prefix, range, 0))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    release.add_permits(1);
    assert!(pending.await.unwrap().unwrap() == shard_indexes("latest", 12));
    // All publishers are now stopped; explicit deletion removes every retained
    // generation, and the reader reports an absent shard.
    krabka_blockstore::delete_tenant_log_index_shard_from_object_store(
        store.as_ref(),
        &prefix,
        "tenant-a",
        range,
    )
    .await
    .unwrap();
    assert!(matches!(
        read_tenant_log_index_shard_from_object_store(store.as_ref(), &prefix, "tenant-a", range,)
            .await,
        Err(krabka_blockstore::LogBlockStoreError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn shard_generation_overflow_fails_without_a_new_publication() {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("logs");
    let range = TimeRange::new(19, 20).unwrap();
    let path = shard_snapshot_path(&prefix, range, u64::MAX);
    store
        .put(
            &path,
            r#"{"format_version":1,"series":[],"blocks":[]}"#.to_string().into(),
        )
        .await
        .unwrap();
    store.clear_put_paths();
    let (labels, blocks) = shard_indexes("api", 1);
    assert!(
        update_tenant_log_index_shard_to_object_store(
            &store,
            &prefix,
            "tenant-a",
            range,
            &BlockIndex::default(),
            &labels,
            &blocks,
        )
        .await
        .is_err()
    );
    assert!(store.put_paths().is_empty());
    assert!(store.get_paths() == vec![path.to_string()]);
    assert!(
        read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", range)
            .await
            .unwrap()
            == (LabelIndex::default(), BlockIndex::default())
    );
}
