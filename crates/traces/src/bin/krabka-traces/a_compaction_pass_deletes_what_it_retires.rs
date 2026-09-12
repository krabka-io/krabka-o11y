//! One compaction pass, run the way the role runs it.
//!
//! The unit pieces are tested through the library's public API. What only this
//! module reaches is the order the role puts them in: the index that no longer
//! names a block is saved before the block's object is deleted, and no object is
//! deleted at all when no save happened.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use assert2::check;
use clap::Parser as _;
use futures::StreamExt as _;
use krabka_blockstore::{BlockWriter, DEFAULT_INDEX_SNAPSHOT_MAX, TraceIndex};
use krabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode,
    blockbuilder::{TRACE_BLOCK_OBJECT_PREFIX, build_blocks},
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use url::Url;

use super::{
    Cli, ConfiguredObjectStore, OverridesProvider, ServiceMetrics, compaction_policy_from_cli,
    limits_from_cli, load_traces_limits_overrides_config, run_compactor_once,
};

const TRACE_INDEX_KEY: &str = "index/traces.json";
const DAY_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// The real wall clock, because the role reads it for the retention cutoff.
fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos(),
    )
    .expect("a clock inside the nanosecond range")
}

fn compactor_cli(retention: &str) -> Cli {
    Cli::try_parse_from([
        "krabka-traces",
        "--target",
        "compactor",
        "--object-store-url",
        "memory:///",
        "--trace-index-key",
        TRACE_INDEX_KEY,
        "--block-retention",
        retention,
    ])
    .expect("the compactor CLI")
}

fn configured(store: Arc<dyn ObjectStore>) -> ConfiguredObjectStore {
    ConfiguredObjectStore {
        store,
        root: Url::parse("memory:///").expect("the memory URL"),
        prefix: Path::default(),
    }
}

fn span_record(trace: u8, start_ns: i64) -> SpanRecord {
    SpanRecord {
        tenant: "tenant-a".into(),
        span: Span {
            trace_id: [trace; 16],
            span_id: [trace; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns,
            duration_ns: 5,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "test".into(),
            instrumentation_version: String::new(),
        },
    }
}

/// Writes one indexed block and publishes the index, as the block builder does.
async fn publish_block(
    store: &Arc<dyn ObjectStore>,
    trace: u8,
    start_ns: i64,
    offset: i64,
) -> String {
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        store,
        TRACE_INDEX_KEY,
        DEFAULT_INDEX_SNAPSHOT_MAX,
    )
    .await
    .expect("the index loads");
    let metas = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[span_record(trace, start_ns)],
        (offset, offset),
    )
    .await
    .expect("the block is written");
    index
        .save_latest_snapshot(store, TRACE_INDEX_KEY)
        .await
        .expect("the index publishes");
    metas[0].object_key.clone()
}

async fn block_keys_in_store(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut keys = Vec::new();
    let mut listing = store.list(Some(&Path::from(TRACE_BLOCK_OBJECT_PREFIX)));
    while let Some(meta) = listing.next().await {
        keys.push(meta.expect("the listing reads").location.to_string());
    }
    keys.sort();
    keys
}

async fn published_block_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        store,
        TRACE_INDEX_KEY,
        DEFAULT_INDEX_SNAPSHOT_MAX,
    )
    .await
    .expect("the index loads");
    let mut keys: Vec<String> = index
        .compaction_candidates()
        .into_iter()
        .map(|candidate| candidate.object_key)
        .collect();
    keys.sort();
    keys
}

/// The pass merges two fresh blocks, expires a third that is past the window,
/// and leaves the bucket holding exactly what the published index names.
#[tokio::test]
async fn a_pass_leaves_the_bucket_holding_exactly_what_the_index_names() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let now = now_ns();
    let first = publish_block(&store, 1, now, 10).await;
    let second = publish_block(&store, 2, now, 20).await;
    let expired = publish_block(&store, 3, now - 30 * DAY_NS, 30).await;
    let cli = compactor_cli("336h");

    let outputs = run_compactor_once(
        &cli,
        &configured(store.clone()),
        compaction_policy_from_cli(&cli),
        &load_overrides(&cli),
        &ServiceMetrics::new(),
    )
    .await
    .expect("the pass runs");

    check!(outputs == 1, "the two fresh blocks merged into one");
    let published = published_block_keys(&store).await;
    check!(published.len() == 1);
    check!(!published.contains(&first));
    check!(!published.contains(&second));
    check!(!published.contains(&expired));
    check!(
        block_keys_in_store(&store).await == published,
        "no retired object is left in the bucket"
    );
}

/// A pass with nothing to do deletes no object. That is the invariant the early
/// return protects: an object may only go once a save has made the index that
/// dropped it durable, and a pass that saved nothing dropped nothing.
#[tokio::test]
async fn a_pass_with_nothing_to_do_deletes_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // One block cannot be a compaction job, because a job merges at least two,
    // and a zero window expires nothing.
    let only = publish_block(&store, 1, now_ns(), 10).await;
    let cli = compactor_cli("0s");
    let before = block_keys_in_store(&store).await;

    let outputs = run_compactor_once(
        &cli,
        &configured(store.clone()),
        compaction_policy_from_cli(&cli),
        &load_overrides(&cli),
        &ServiceMetrics::new(),
    )
    .await
    .expect("the pass runs");

    check!(outputs == 0);
    check!(block_keys_in_store(&store).await == before);
    check!(published_block_keys(&store).await == vec![only]);
}

fn load_overrides(cli: &Cli) -> OverridesProvider {
    load_traces_limits_overrides_config(None, limits_from_cli(cli)).expect("the overrides load")
}
