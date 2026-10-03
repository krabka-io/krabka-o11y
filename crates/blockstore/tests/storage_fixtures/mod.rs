//! Stores that the storage audit and repair suites share.
//!
//! Every fixture starts from a healthy store for one signal and tenant, then
//! injects one fault. The keys follow the grammar of the service that owns
//! the signal, so the audit classifies them as it would in production.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
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
    StorageSignal, TimeRange, TraceBlockStats, TraceIndex, labels,
    log_tenant_index_shard_manifest_object_path, write_log_block_to_object_store,
    write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_catalog_to_object_store,
};
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
use serde::Serialize;
use serde_wincode::{SerdeCompat, wincode::Serialize as _};

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

// The block kind of a metrics manifest. The codec writes a variant as its
// index, and `Float` is the first variant of `MetricBlockKind`.
#[derive(Serialize)]
enum MetricKind {
    Float,
}

// A metrics `.index` manifest in the field order of `CompactionIndexManifest`.
#[derive(Serialize)]
struct MetricsManifest<'a> {
    tenant: &'a str,
    kind: MetricKind,
    block_key: &'a str,
    index_key: &'a str,
    level: u32,
    first_offset: i64,
    last_offset: i64,
    row_count: usize,
    min_ts: i64,
    max_ts: i64,
    fingerprints: Vec<u64>,
    series: Vec<(u64, Labels)>,
}

// The bytes of a metrics `.index` manifest that names `tenant`, `block_key`
// and `index_key`.
pub fn metrics_manifest(tenant: &str, block_key: &str, index_key: &str) -> Vec<u8> {
    let manifest = MetricsManifest {
        tenant,
        kind: MetricKind::Float,
        block_key,
        index_key,
        level: 0,
        first_offset: 0,
        last_offset: 9,
        row_count: 1,
        min_ts: 10,
        max_ts: 10,
        fingerprints: vec![7],
        series: vec![(7, Labels::from_pairs([("__name__", "up")]))],
    };
    SerdeCompat::<MetricsManifest>::serialize(&manifest).unwrap()
}

// The bytes of an empty `.symdb` symbol table, in the field order of
// `SymbolDb`.
pub fn symbol_table() -> Vec<u8> {
    type Shape = (
        Vec<String>,
        Vec<(u32, u32, u32, i64)>,
        Vec<(u64, u32, Vec<(u32, i32)>)>,
        Vec<(u64, u64, u64, u32, u32, u8)>,
        HashMap<u64, Vec<(i32, i32)>>,
    );
    let empty: Shape = (
        vec![String::new()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        HashMap::new(),
    );
    SerdeCompat::<Shape>::serialize(&empty).unwrap()
}

// The tenant segment of a block key.
pub fn key_tenant(key: &str) -> &str {
    key.split('/').nth(1).unwrap()
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

pub async fn put_bytes(store: &Arc<dyn ObjectStore>, key: &str, bytes: impl AsRef<[u8]>) {
    store
        .put(&Path::from(key), PutPayload::from(bytes.as_ref().to_vec()))
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
                let sidecar = sidecar_key(signal, key);
                let manifest = metrics_manifest(key_tenant(key), key, &sidecar);
                put_bytes(store, &sidecar, manifest).await;
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
                put_bytes(store, &sidecar_key(signal, key), symbol_table()).await;
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

// The faults below each add a block of tenant `t` or `u` to a healthy store
// of their signal. Each block is live: a reader can reach it. An audit that
// misreads the index calls it an orphan, and a repair then deletes it.

// A logs block that only a shard manifest names, and a shard catalog that
// names that shard manifest, which the store does not hold. Returns the key
// of the shard manifest and of the block.
pub async fn missing_shard(store: &Arc<dyn ObjectStore>) -> (String, String) {
    let block = block_key(StorageSignal::Logs, "t", 10, 19);
    put_block(store, StorageSignal::Logs, &block).await;
    let range = TimeRange::new(10, 19).unwrap();
    write_tenant_log_index_shard_catalog_to_object_store(
        store.as_ref(),
        &Path::default(),
        "t",
        &[range],
    )
    .await
    .unwrap();
    let shard = log_tenant_index_shard_manifest_object_path(&Path::default(), "t", range);
    (shard.to_string(), block)
}

// A metrics block whose manifest has an upper-case `.INDEX` extension, which
// the metrics loaders read. Returns the key of the block.
pub async fn upper_case_manifest(store: &Arc<dyn ObjectStore>) -> String {
    let block = block_key(StorageSignal::Metrics, "t", 10, 19);
    put_block(store, StorageSignal::Metrics, &block).await;
    let manifest = format!("{}.INDEX", block.trim_end_matches(".parquet"));
    put_bytes(store, &manifest, metrics_manifest("t", &block, &manifest)).await;
    block
}

// A traces or profiles block of tenant `u` that the index of tenant `t`
// names, and the index of `u` does not. Returns the key of the block.
pub async fn foreign_entry(store: &Arc<dyn ObjectStore>, signal: StorageSignal) -> String {
    let foreign = block_key(signal, "u", 10, 19);
    put_block(store, signal, &foreign).await;
    let t_blocks = [block_key(signal, "t", 0, 9), foreign.clone()];
    let u_blocks = [block_key(signal, "u", 0, 9)];
    let sidecars = t_blocks.iter().chain(&u_blocks).cloned().collect();
    publish(
        store,
        signal,
        &[("t", &t_blocks), ("u", &u_blocks)],
        &sidecars,
    )
    .await;
    foreign
}
