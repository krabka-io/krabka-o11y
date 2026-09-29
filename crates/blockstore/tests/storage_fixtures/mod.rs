//! Stores that the storage audit and repair suites share.
//!
//! Every fixture starts from a healthy store for one signal and tenant, then
//! injects one fault. The keys follow the grammar of the service that owns
//! the signal, so the audit classifies them as it would in production.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use arrow::{
    array::{Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::StreamExt as _;
use krabka_blockstore::{
    BlockDescriptor, BlockIndex, BlockKey, BlockLevel, BlockMeta, BlockWriter, COL_FINGERPRINT,
    COL_TIMESTAMP, LabelIndex, Labels, LogBlockIndex, LogRow, ProfileIndex, ShardedTraceBloom,
    StorageSignal, TimeRange, TraceBlockStats, TraceIndex, labels, write_log_block_to_object_store,
    write_tenant_log_index_manifest_to_object_store,
};
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};

pub const TRACE_INDEX: &str = "index/traces.json";
pub const PROFILE_INDEX: &str = "index/profiles.json";
pub const CPU_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

pub fn store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

// A clock two hours ahead, so every object the fixture wrote is older than
// the default one-hour grace window.
pub fn later() -> SystemTime {
    SystemTime::now() + Duration::from_hours(2)
}

// The key of a block of `signal` for `tenant` that covers WAL offsets
// `first..=last` of partition 0.
pub fn block_key(signal: StorageSignal, tenant: &str, first: i64, last: i64) -> String {
    match signal {
        StorageSignal::Metrics => format!("metrics/{tenant}/float/{first:020}-{last:020}.parquet"),
        StorageSignal::Traces => format!("traces/{tenant}/00000/{first:020}-{last:020}-0.parquet"),
        StorageSignal::Profiles => {
            format!("blocks/{tenant}/00000/{first:020}-{last:020}-0-100.parquet")
        }
        StorageSignal::Logs => log_key(tenant, first, last).object_key(),
    }
}

// The sidecar of a metrics or profiles block: `.index` or `.symdb`.
pub fn sidecar_key(signal: StorageSignal, block: &str) -> String {
    match signal {
        StorageSignal::Metrics => format!("{}.index", block.trim_end_matches(".parquet")),
        _ => format!("{block}.symdb"),
    }
}

fn log_key(tenant: &str, first: i64, last: i64) -> BlockKey {
    BlockKey::new(tenant, 0, first, last, TimeRange::new(10, 19).unwrap())
}

fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
    ]))
}

// Writes a real, readable block of `signal` at `key`.
pub async fn put_block(store: &Arc<dyn ObjectStore>, signal: StorageSignal, key: &str) {
    if signal == StorageSignal::Logs {
        let parsed = parse_log_key(key);
        let fingerprint = krabka_blockstore::series_fingerprint(&labels([("app", "api")]));
        write_log_block_to_object_store(
            store.as_ref(),
            &Path::default(),
            &parsed,
            vec![LogRow::new(fingerprint, 10, "line", BTreeMap::new())],
        )
        .await
        .unwrap();
        return;
    }
    let batch = RecordBatch::try_new(
        series_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![7_u64])),
            Arc::new(Int64Array::from(vec![10_i64])),
        ],
    )
    .unwrap();
    BlockWriter::new(Arc::clone(store))
        .write_block("t", key, series_schema(), &[batch])
        .await
        .unwrap();
}

fn parse_log_key(key: &str) -> BlockKey {
    let offsets = key
        .split('/')
        .find_map(|segment| segment.strip_prefix("offsets="))
        .unwrap();
    let tenant = key
        .split('/')
        .next()
        .and_then(|segment| segment.strip_prefix("tenant="))
        .unwrap();
    let (first, last) = offsets.split_once('-').unwrap();
    log_key(tenant, first.parse().unwrap(), last.parse().unwrap())
}

pub async fn put_bytes(store: &Arc<dyn ObjectStore>, key: &str, bytes: &'static [u8]) {
    store
        .put(&Path::from(key), PutPayload::from_static(bytes))
        .await
        .unwrap();
}

// Publishes `blocks` of `tenant` as the whole index of `signal`, with the
// sidecars that metrics and profiles need.
//
// A sidecar is written only for a block in `with_sidecar`. The index names
// every block in `blocks`, whether the store holds it or not.
pub async fn publish(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    tenants: &[(&str, &[String])],
    with_sidecar: &BTreeSet<String>,
) {
    match signal {
        StorageSignal::Metrics => {
            for key in with_sidecar {
                put_bytes(store, &sidecar_key(signal, key), b"index").await;
            }
        }
        StorageSignal::Traces => {
            let mut index = TraceIndex::new();
            for (tenant, blocks) in tenants {
                for key in *blocks {
                    index.add_trace_block(tenant, trace_stats(key));
                }
            }
            index
                .save_latest_snapshot(store, TRACE_INDEX)
                .await
                .unwrap();
        }
        StorageSignal::Profiles => {
            let mut index = ProfileIndex::new();
            let series = Labels::from_pairs([("__profile_type__", CPU_TYPE.to_string())]);
            for (tenant, blocks) in tenants {
                index
                    .add_series(tenant, series.fingerprint(), &series)
                    .unwrap();
                for key in *blocks {
                    BlockIndex::add_block(&mut index, &block_meta(tenant, key, &series));
                    index.add_profile_block(tenant, key, vec![1]);
                }
            }
            for key in with_sidecar {
                put_bytes(store, &sidecar_key(signal, key), b"symbols").await;
            }
            index
                .save_latest_snapshot(store, PROFILE_INDEX)
                .await
                .unwrap();
        }
        StorageSignal::Logs => {
            for (tenant, blocks) in tenants {
                let mut label_index = LabelIndex::default();
                let fingerprint = label_index.insert_series(*tenant, labels([("app", "api")]));
                let mut block_index = LogBlockIndex::default();
                for key in *blocks {
                    block_index.insert(BlockDescriptor::new(
                        parse_log_key(key),
                        BTreeSet::from([fingerprint]),
                    ));
                }
                write_tenant_log_index_manifest_to_object_store(
                    store.as_ref(),
                    &Path::default(),
                    tenant,
                    &label_index,
                    &block_index,
                )
                .await
                .unwrap();
            }
        }
    }
}

fn trace_stats(object_key: &str) -> TraceBlockStats {
    TraceBlockStats {
        object_key: object_key.to_string(),
        min_ts: 0,
        max_ts: 100,
        bloom: ShardedTraceBloom::match_all_with_tempo_defaults(),
        tag_names: BTreeSet::new(),
        tag_values: BTreeMap::new(),
        row_count: 1,
        level: BlockLevel::INGESTED,
    }
}

fn block_meta(tenant: &str, object_key: &str, series: &Labels) -> BlockMeta {
    BlockMeta {
        tenant: tenant.to_string(),
        object_key: object_key.to_string(),
        min_ts: 0,
        max_ts: 100,
        row_count: 1,
        fingerprints: vec![series.fingerprint()],
        level: BlockLevel::INGESTED,
    }
}

// A healthy store of `signal`: for each tenant, one published block with its
// sidecar. Returns the published block keys.
pub async fn healthy(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    tenants: &[&str],
) -> Vec<String> {
    let blocks: Vec<Vec<String>> = tenants
        .iter()
        .map(|tenant| vec![block_key(signal, tenant, 0, 9)])
        .collect();
    for key in blocks.iter().flatten() {
        put_block(store, signal, key).await;
    }
    let published: Vec<(&str, &[String])> = tenants
        .iter()
        .copied()
        .zip(blocks.iter().map(Vec::as_slice))
        .collect();
    let sidecars = blocks.iter().flatten().cloned().collect();
    publish(store, signal, &published, &sidecars).await;
    blocks.into_iter().flatten().collect()
}

pub async fn listed_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut keys = store
        .list(None)
        .map(|meta| meta.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    keys.sort();
    keys
}
