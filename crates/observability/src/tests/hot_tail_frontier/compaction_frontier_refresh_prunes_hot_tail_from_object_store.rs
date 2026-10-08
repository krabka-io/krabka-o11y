use assert2::assert;
use krabka_blockstore::{LogRow, labels};

use super::*;
use crate::{
    LokiDirection, LokiStreamEncoding, ServiceConfig,
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
        r#"{app="api"}"#,
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
        (LokiDirection::Forward, Some(10), None, Some(30)),
        LokiStreamEncoding::Folded,
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
    let store = Arc::new(if blocked_load {
        store.with_get_barrier(
            krabka_blockstore::log_tenant_index_shard_manifest_object_path(
                &prefix,
                "tenant-a",
                TimeRange::new(19, 19).unwrap(),
            )
            .to_string(),
            entered.clone(),
            release.clone(),
        )
    } else {
        store
    });
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
