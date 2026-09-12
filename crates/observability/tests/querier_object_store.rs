//! Queriers built over an object store, and the tenant manifest each request reads.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogRow, TimeRange, labels,
    log_block_object_path, write_log_block, write_log_block_to_object_store,
    write_log_index_manifest, write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_to_object_store, write_tenant_log_index_shards_to_object_store,
};
use krabka_observability::{
    InMemoryWalSink, LogWalSink, QuerierIndexSource, QuerierState, Role, ServiceConfig,
    ServiceDependencies, WalLogRecord, build_service_router, loki_router,
};
use krabka_units::convert::ByteSizeExt as _;
use object_store::{ObjectStoreExt as _, local::LocalFileSystem, path::Path as ObjectPath};
use serde_json::json;
use support::{
    expected_api_error, expected_loki_mixed_stats_with, expected_loki_stats_with, json_body,
    tenant_object_store_shard_catalog_config_fixture,
};
use tower::ServiceExt as _;

#[tokio::test]
async fn query_endpoint_can_load_indexes_from_persisted_manifest() {
    let state = persisted_fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_manifest() {
    let state = tenant_object_store_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_shard() {
    let state = tenant_object_store_shard_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_shard_catalog() {
    let state = tenant_object_store_shard_catalog_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_build_querier_from_object_store_shard_catalog_config() {
    let (state, _dir) = tenant_object_store_shard_catalog_config_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn configured_object_store_query_returns_partial_warning_for_missing_block() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let readable_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .await
    .unwrap();
    let readable_block_bytes = readable_block.size.bytes_u64();
    let missing_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([api]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(readable_block);
    block_index.insert(missing_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(readable_block_bytes, 1, 2)
                },
                "warnings": [
                    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet"
                ]
            })
    );
}

#[tokio::test]
async fn configured_object_store_backward_limited_query_stops_after_newest_block() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let missing_old_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        BTreeSet::from([api]),
    );
    let newest_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![
            LogRow::new(api, 20, "api older error", BTreeMap::new()),
            LogRow::new(api, 29, "api newest error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let newest_block_bytes = newest_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(missing_old_block);
    block_index.insert(newest_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=backward&limit=1",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["29", "api newest error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(newest_block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_query_merges_hot_tail_with_source_split_stats() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let cold_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api cold error", BTreeMap::new())],
    )
    .await
    .unwrap();
    let cold_block_bytes = cold_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(cold_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-object-hot-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_hot_tail(hot_tail, 19),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api cold error"],
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(cold_block_bytes, 1, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_metric_query_returns_partial_warning_for_missing_block() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let readable_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let readable_block_bytes = readable_block.size.bytes_u64();
    let missing_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([api]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(readable_block);
    block_index.insert(missing_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query_range?query=count_over_time(%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D)&start=30&end=30&step=1ns",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.000_000_03, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(readable_block_bytes, 1, 2)
                },
                "warnings": [
                    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet"
                ]
            })
    );
}

#[tokio::test]
async fn configured_object_store_index_stats_endpoint_counts_entries_from_object_store_blocks() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let expected_block_bytes = api_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 2,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn configured_object_store_index_stats_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let expected_block_bytes = tenant_b_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=20&end=29")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 1,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn configured_object_store_index_volume_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let expected_block_bytes = tenant_b_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=20&end=29")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "env": "stage"
                            },
                            "value": [29, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_patterns_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new(
            "tenant-b",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                tenant_b_api,
                100_000_000,
                "status=500 user=123 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                tenant_b_api,
                1_100_000_000,
                "status=503 user=456 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn configured_object_store_detected_fields_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            r#"{"status":500}"#,
            BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=20&end=29&limit=10")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["structured_metadata"]
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn configured_object_store_querier_loads_manifest_for_request_tenant_header() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let prod_api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let stage_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let prod_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(
            prod_api,
            19,
            "tenant-a api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let stage_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            stage_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    let stage_block_bytes = stage_block.size.bytes_u64();
    block_index.insert(prod_block);
    block_index.insert(stage_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=29",
                )
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                ["29", "tenant-b api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(stage_block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_labels_endpoint_loads_manifest_for_request_tenant_header() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &BlockIndex::default(),
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({"status": "success", "data": ["app", "env"]}));
}

#[tokio::test]
async fn configured_object_store_shard_catalog_querier_loads_shards_for_request_tenant_header() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    let tenant_b_block_bytes = tenant_b_block.size.bytes_u64();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &[TimeRange::new(20, 29).unwrap()],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=29",
                )
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                ["29", "tenant-b api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(tenant_b_block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_shard_catalog_labels_endpoint_loads_request_tenant_shards() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &[TimeRange::new(20, 29).unwrap()],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels?start=20&end=29")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({"status": "success", "data": ["app", "env"]}));
}

fn persisted_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();

    QuerierState::from_manifest(dir).unwrap()
}

async fn tenant_object_store_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store(dir, &store, &prefix, "tenant-a")
        .await
        .unwrap()
}

async fn tenant_object_store_shard_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let shard_range = TimeRange::new(0, 30).unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        shard_range,
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store_shard(dir, &store, &prefix, "tenant-a", shard_range)
        .await
        .unwrap()
}

async fn tenant_object_store_shard_catalog_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &api_block.key,
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &worker_block.key,
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .await
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(0, 19).unwrap(),
            TimeRange::new(20, 29).unwrap(),
        ],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store_shards(
        dir,
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(0, 19).unwrap(),
    )
    .await
    .unwrap()
}

/// A querier over an index that still names a block whose object a retention
/// sweep deleted.
///
/// Two blocks are written and the manifest records both. The second block's
/// object is then deleted, which is what a sweep does to a live querier: the
/// index is unchanged, and the bytes are gone. Both blocks hold the `api`
/// series, so every query for `api` plans both and meets the gap.
async fn retention_swept_fixture() -> (axum::Router, u64, u64) {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let web = label_index.insert_series("tenant-a", labels([("app", "web"), ("env", "prod")]));

    let surviving_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, r#"{"status":200}"#, BTreeMap::new())],
    )
    .await
    .unwrap();

    let swept_key = BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap());
    let swept_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &swept_key,
        vec![
            LogRow::new(api, 20, r#"{"status":500}"#, BTreeMap::new()),
            LogRow::new(web, 29, r#"{"status":503}"#, BTreeMap::new()),
        ],
    )
    .await
    .unwrap();

    let surviving_bytes = surviving_block.size.bytes_u64();
    let swept_bytes = swept_block.size.bytes_u64();
    let mut block_index = BlockIndex::default();
    block_index.insert(surviving_block);
    block_index.insert(swept_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    // The sweep itself. The manifest still names the block.
    store
        .delete(&log_block_object_path(&prefix, &swept_key))
        .await
        .unwrap();

    let app = build_service_router(
        &retention_config(&object_dir.display().to_string(), data_root, &prefix),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();
    (app, surviving_bytes, swept_bytes)
}

fn retention_config(
    object_dir: &str,
    data_root: std::path::PathBuf,
    prefix: &ObjectPath,
) -> ServiceConfig {
    ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{object_dir}")),
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    }
}

/// A retention sweep deletes block objects while queriers run, so every read
/// surface that plans a block can meet one that is gone. None of them may
/// fail the whole request for it: a valid query answers with the blocks that
/// remain.
///
/// The metadata surfaces degrade differently from the analytics ones, and
/// both shapes are pinned here. `/labels`, `/label/{name}/values` and
/// `/series` fall back to the fingerprints the index still records for the
/// swept block, so `web` survives in the answer even though its rows are
/// gone. `/index/stats`, `/patterns` and `/detected_fields` have no such
/// index-level fallback, so they answer from the surviving block alone.
#[tokio::test]
async fn a_retention_swept_block_degrades_every_read_surface_instead_of_failing() {
    let (app, surviving_bytes, swept_bytes) = retention_swept_fixture().await;

    let cases = vec![
        (
            "/loki/api/v1/labels?start=10&end=29",
            json!({"status": "success", "data": ["app", "env"]}),
        ),
        (
            "/loki/api/v1/label/app/values?start=10&end=29",
            json!({"status": "success", "data": ["api", "web"]}),
        ),
        (
            "/loki/api/v1/series?match%5B%5D=%7Benv%3D%22prod%22%7D&start=10&end=29",
            json!({
                "status": "success",
                "data": [
                    {"app": "api", "env": "prod"},
                    {"app": "web", "env": "prod"},
                ],
            }),
        ),
        (
            "/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=10&end=29",
            json!({
                "streams": 1,
                "chunks": 2,
                "entries": 1,
                "bytes": surviving_bytes + swept_bytes,
            }),
        ),
        (
            "/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=10&end=29&step=1000000000",
            json!({
                "status": "success",
                "data": [
                    {"pattern": r#"{"status":"<_>"}"#, "samples": [[0, 1]]},
                ],
            }),
        ),
        (
            "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=10&end=29&limit=10",
            json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null,
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"],
                    },
                ],
                "limit": 10,
            }),
        ),
    ];

    for (uri, expected) in cases {
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

        assert!(
            response.status() == StatusCode::OK,
            "{uri} answers 200 despite the swept block"
        );
        assert!(json_body(response).await == expected, "{uri}");
    }
}

/// The tolerance covers an absent block object and nothing else.
///
/// Here the block object is PRESENT and holds bytes that are not a Parquet
/// block, which is a real fault in the stored data rather than a sweep. The
/// request fails, and it must keep failing: a decode error that degraded to a
/// skip would shorten every result with nothing to say so.
///
/// The status pins today's mapping, where `HttpQueryError::BlockStore` lands
/// in the `BAD_REQUEST` arm. The mapping itself is a separate question, since
/// a malformed stored block is a server-side fault and not a client error.
#[tokio::test]
async fn a_present_but_malformed_block_still_fails_the_request() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap());
    let block = write_log_block_to_object_store(
        &store,
        &prefix,
        &key,
        vec![LogRow::new(api, 10, r#"{"status":200}"#, BTreeMap::new())],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    // The object stays, and its bytes stop being a block.
    store
        .put(
            &log_block_object_path(&prefix, &key),
            b"not a parquet block".to_vec().into(),
        )
        .await
        .unwrap();

    let app = build_service_router(
        &retention_config(&object_dir.display().to_string(), data_root, &prefix),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
}
