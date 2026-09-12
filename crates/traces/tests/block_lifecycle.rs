//! The lifecycle a span block has after it is written.
//!
//! Three things delete a block, and each one has to leave the index and object
//! storage agreeing: compaction retires the inputs it merged, retention retires
//! a block that has aged out of its tenant's window, and the orphan sweep
//! retires an object no index names at all. Every test here checks the objects
//! the store holds afterwards, because the object set is what a querier and an
//! operator's storage bill both read.

use std::{collections::BTreeSet, fmt::Write as _, sync::Arc, time::SystemTime};

use assert2::check;
use futures::StreamExt as _;
use krabka_blockstore::{
    BlockDeletionReport, BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy,
    DEFAULT_BLOCK_SWEEP_GRACE, ExpiredBlock, OrphanSweepStats, TraceIndex, read_block,
};
use krabka_traces::{
    AttrValue, KeyValue, Limits, Span, SpanKind, SpanRecord, StatusCode,
    blockbuilder::{TRACE_BLOCK_OBJECT_PREFIX, build_blocks},
    compactor::{
        BlockSweepError, compact_once, delete_trace_blocks, expire_trace_blocks,
        sweep_orphaned_trace_blocks,
    },
    ids::UnixNano,
    limits::OverridesProvider,
};
use krabka_units::{Time, convert::TimeExt, days, hours};
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutPayload, local::LocalFileSystem, memory::InMemory,
    path::Path,
};

const DAY_NS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// A fixed epoch-nanosecond "now". A traces block counts its bounds in
/// nanoseconds, and a test that let the wall clock in would measure the window
/// against a moving instant.
const NOW_NS: i64 = 1_700_000_000 * 1_000_000_000;

fn span_record(tenant: &str, trace: u8, start_ns: i64) -> SpanRecord {
    SpanRecord {
        tenant: tenant.to_string(),
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

/// Writes one block for `tenant` whose newest span starts at `start_ns`, and
/// registers it in `index`. Returns its object key.
///
/// `offset` separates one block's key from the next, as the WAL offsets do in
/// production.
async fn write_block(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    trace: u8,
    start_ns: i64,
    offset: i64,
) -> String {
    let metas = build_blocks(
        writer,
        index,
        tenant,
        7,
        &[span_record(tenant, trace, start_ns)],
        (offset, offset),
    )
    .await
    .expect("the block is written");
    check!(metas.len() == 1, "one flush window is one block");
    metas[0].object_key.clone()
}

/// Every block key the index names, in a stable order.
fn indexed_block_keys(index: &TraceIndex) -> Vec<String> {
    let mut keys: Vec<String> = index
        .compaction_candidates()
        .into_iter()
        .map(|candidate| candidate.object_key)
        .collect();
    keys.sort();
    keys
}

async fn keys_under(store: &Arc<dyn ObjectStore>, prefix: &str) -> BTreeSet<String> {
    let mut listing = store.list(Some(&Path::from(prefix)));
    let mut keys = BTreeSet::new();
    while let Some(meta) = listing.next().await {
        keys.insert(meta.expect("the listing reads").location.to_string());
    }
    keys
}

fn windows(entries: &[(&str, Time)]) -> OverridesProvider {
    let mut yaml = String::from("overrides:\n");
    for (tenant, window) in entries {
        let secs = window.secs_i64();
        writeln!(yaml, "  {tenant}:\n    block_retention: {secs}s").expect("a string writes");
    }
    OverridesProvider::from_yaml(&yaml).expect("the overrides parse")
}

async fn sweep(
    store: &Arc<dyn ObjectStore>,
    trace_index_key: &str,
    index: &TraceIndex,
    now: SystemTime,
) -> OrphanSweepStats {
    sweep_orphaned_trace_blocks(
        store,
        "",
        trace_index_key,
        index,
        DEFAULT_BLOCK_SWEEP_GRACE,
        now,
    )
    .await
    .expect("the sweep runs")
}

/// A policy wide enough that only the fan-in cap decides what merges: no row
/// target and no time bucketing in the way.
fn wide_policy() -> CompactionPolicy {
    CompactionPolicy::new(
        8,
        usize::MAX,
        BlockLevel(4),
        days(36_500),
        BlockTimestampUnit::Nanos,
    )
}

/// The window is measured from the newest span in the block, and a block inside
/// it is still readable data that a query over the window would need.
#[tokio::test]
async fn a_block_past_its_window_is_dropped_and_deleted_and_one_inside_it_survives() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let old = write_block(&writer, &mut index, "tenant-a", 1, NOW_NS - 30 * DAY_NS, 10).await;
    let recent = write_block(&writer, &mut index, "tenant-a", 2, NOW_NS - DAY_NS, 20).await;

    let expired = expire_trace_blocks(
        &mut index,
        UnixNano(NOW_NS),
        &windows(&[("tenant-a", days(14))]),
    );

    check!(
        expired
            == vec![ExpiredBlock {
                tenant: "tenant-a".to_string(),
                object_key: old.clone(),
            }]
    );
    check!(indexed_block_keys(&index) == vec![recent.clone()]);

    // The index that no longer names the block is what makes the object safe to
    // delete. In production a snapshot save sits here.
    let report = delete_trace_blocks(&store, std::slice::from_ref(&old)).await;

    check!(
        report
            == BlockDeletionReport {
                blocks_deleted: 1,
                ..BlockDeletionReport::default()
            }
    );
    check!(keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await == BTreeSet::from([recent.clone()]));
    // The survivor is still a block a querier can read, not just a key.
    let rows: usize = read_block(store, &recent)
        .await
        .expect("the surviving block reads")
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    check!(rows == 1);
}

/// A snapshot save merges this writer's state into whatever is durable, so a
/// removal that was not pinned would come straight back on the next save. This
/// is the expiry counterpart of the compaction case
/// `compaction_removals_are_not_resurrected_by_the_merge`.
#[tokio::test]
async fn an_expired_block_does_not_come_back_through_a_snapshot_merge() {
    const KEY: &str = "index/traces.json";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let old = write_block(&writer, &mut index, "tenant-a", 1, NOW_NS - 30 * DAY_NS, 10).await;
    let recent = write_block(&writer, &mut index, "tenant-a", 2, NOW_NS - DAY_NS, 20).await;
    index
        .save_latest_snapshot(&store, KEY)
        .await
        .expect("the first snapshot saves");

    let expired = expire_trace_blocks(
        &mut index,
        UnixNano(NOW_NS),
        &windows(&[("tenant-a", days(14))]),
    );
    index
        .save_latest_snapshot(&store, KEY)
        .await
        .expect("the expiry snapshot saves");

    check!(expired.len() == 1);
    let loaded = TraceIndex::load_latest_snapshot(&store, KEY)
        .await
        .expect("the snapshot loads");
    check!(indexed_block_keys(&loaded) == vec![recent.clone()]);

    // The removal is durable now, so a later save must not undo it either.
    index
        .save_latest_snapshot(&store, KEY)
        .await
        .expect("the next snapshot saves");
    let loaded = TraceIndex::load_latest_snapshot(&store, KEY)
        .await
        .expect("the snapshot loads again");
    check!(indexed_block_keys(&loaded) == vec![recent]);
    check!(!indexed_block_keys(&loaded).contains(&old));
}

/// Without this, every compaction pass permanently doubles the storage of the
/// range it merged: the index names the output and the inputs stay in the
/// bucket forever.
#[tokio::test]
async fn compaction_inputs_are_deleted_and_the_merged_output_still_reads() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let first = write_block(&writer, &mut index, "tenant-a", 1, NOW_NS, 10).await;
    let second = write_block(&writer, &mut index, "tenant-a", 2, NOW_NS, 20).await;

    let pass = compact_once(store.clone(), &writer, &mut index, "", wide_policy())
        .await
        .expect("the pass compacts");

    check!(pass.outputs.len() == 1);
    let output = pass.outputs[0].object_key.clone();
    let mut retired = pass.retired_inputs.clone();
    retired.sort();
    let mut inputs = vec![first, second];
    inputs.sort();
    check!(retired == inputs);
    check!(indexed_block_keys(&index) == vec![output.clone()]);

    let report = delete_trace_blocks(&store, &pass.retired_inputs).await;

    check!(
        report
            == BlockDeletionReport {
                blocks_deleted: 2,
                ..BlockDeletionReport::default()
            }
    );
    check!(keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await == BTreeSet::from([output.clone()]));
    let rows: usize = read_block(store, &output)
        .await
        .expect("the merged block reads")
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    check!(rows == 2, "both traces are in the replacement block");
}

/// The sweep keeps what the index names, keeps what is too new to judge, and
/// deletes the rest.
#[tokio::test]
async fn the_orphan_sweep_deletes_only_the_blocks_no_index_names() {
    const KEY: &str = "index/traces.json";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let live = write_block(&writer, &mut index, "tenant-a", 1, NOW_NS, 10).await;
    index
        .save_latest_snapshot(&store, KEY)
        .await
        .expect("the snapshot saves");
    let orphan = format!("{TRACE_BLOCK_OBJECT_PREFIX}/tenant-a/00007/orphaned.parquet");
    store
        .put(&Path::from(orphan.as_str()), PutPayload::from_static(b"x"))
        .await
        .expect("the orphan is written");
    let before = keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await;
    check!(before == BTreeSet::from([live.clone(), orphan.clone()]));

    // Nothing here is older than the grace window, and a block a writer has put
    // and not yet published looks exactly like an orphan, so nothing goes.
    let fresh = sweep(&store, KEY, &index, SystemTime::now()).await;

    check!(fresh.deleted == 0);
    check!(
        fresh.kept_within_grace == 1,
        "the orphan is too new to judge"
    );
    check!(fresh.live == 1, "the index names the other block");
    check!(keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await == before);

    // Two grace windows on, the orphan is old enough to judge.
    let aged = sweep(
        &store,
        KEY,
        &index,
        SystemTime::now() + 2 * DEFAULT_BLOCK_SWEEP_GRACE.to_std(),
    )
    .await;

    check!(aged.deleted == 1);
    check!(keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await == BTreeSet::from([live]));
}

/// The index's own objects are not blocks, and no index names them, so a sweep
/// that could reach them would delete the index. Every block would then stay in
/// the bucket, unreferenced and unqueryable, with nothing to report the loss,
/// and the next sweep would delete the blocks too because nothing named them.
///
/// An operator can put the index there with `--trace-index-key`, so the sweep
/// refuses rather than trusting the configuration.
#[tokio::test]
async fn the_orphan_sweep_refuses_to_run_when_the_index_is_inside_the_block_prefix() {
    // Inside the block prefix, which is the worst case `--trace-index-key`
    // allows.
    const KEY: &str = "traces/index.json";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    write_block(&writer, &mut index, "tenant-a", 1, NOW_NS, 10).await;
    index
        .save_latest_snapshot(&store, KEY)
        .await
        .expect("the snapshot saves");
    let before = keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await;
    let index_objects: BTreeSet<String> = before
        .iter()
        .filter(|key| key.starts_with("traces/index/"))
        .cloned()
        .collect();
    check!(
        !index_objects.is_empty(),
        "the index has objects of its own to lose"
    );

    let refused = sweep_orphaned_trace_blocks(
        &store,
        "",
        KEY,
        &index,
        DEFAULT_BLOCK_SWEEP_GRACE,
        SystemTime::now() + 2 * DEFAULT_BLOCK_SWEEP_GRACE.to_std(),
    )
    .await;

    assert2::assert!(matches!(
        refused,
        Err(BlockSweepError::IndexInsideBlockPrefix { .. })
    ));
    check!(
        keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX).await == before,
        "the index and the blocks are all still there"
    );
    // The index still loads, which is the property the refusal protects.
    let loaded = TraceIndex::load_latest_snapshot(&store, KEY)
        .await
        .expect("the index still loads");
    check!(indexed_block_keys(&loaded) == indexed_block_keys(&index));
}

/// Retention is per tenant, and zero is "keep forever". Reading the sentinel
/// the other way round would delete every block a tenant that configured
/// nothing has.
#[tokio::test]
async fn each_tenant_expires_by_its_own_window_and_a_zero_window_expires_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let short = write_block(&writer, &mut index, "short", 1, NOW_NS - 30 * DAY_NS, 10).await;
    let long = write_block(&writer, &mut index, "long", 2, NOW_NS - 30 * DAY_NS, 20).await;
    let forever = write_block(&writer, &mut index, "forever", 3, NOW_NS - 30 * DAY_NS, 30).await;

    let expired = expire_trace_blocks(
        &mut index,
        UnixNano(NOW_NS),
        &windows(&[
            ("short", hours(48)),
            ("long", days(365)),
            ("forever", <Time as TimeExt>::ZERO),
        ]),
    );

    check!(
        expired
            == vec![ExpiredBlock {
                tenant: "short".to_string(),
                object_key: short,
            }]
    );
    let mut survivors = vec![long, forever];
    survivors.sort();
    check!(indexed_block_keys(&index) == survivors);
}

/// A deletion pass that was interrupted, and a pass that runs twice, both reach
/// objects that are already gone. Neither is a fault, and neither may stop the
/// rest of the pass.
///
/// `InMemory::delete_stream` reports success for a key it never held, so it
/// cannot show this. A real filesystem raises the `NotFound` this tolerates.
#[tokio::test]
async fn an_object_that_is_already_gone_is_not_a_deletion_failure() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("a local store"));
    let present = format!("{TRACE_BLOCK_OBJECT_PREFIX}/tenant-a/00007/present.parquet");
    let absent = format!("{TRACE_BLOCK_OBJECT_PREFIX}/tenant-a/00007/absent.parquet");
    store
        .put(&Path::from(present.as_str()), PutPayload::from_static(b"x"))
        .await
        .expect("the object is written");

    let report = delete_trace_blocks(&store, &[absent, present]).await;

    check!(
        report
            == BlockDeletionReport {
                blocks_deleted: 1,
                objects_absent: 1,
                ..BlockDeletionReport::default()
            }
    );
    check!(
        keys_under(&store, TRACE_BLOCK_OBJECT_PREFIX)
            .await
            .is_empty()
    );
}

/// The default window is Tempo's, and it is what every tenant the runtime
/// overrides file does not name reads.
#[test]
fn the_default_window_is_tempos_and_covers_the_tenants_no_file_names() {
    let provider = OverridesProvider::new(Limits::default());

    check!(provider.for_tenant("unlisted").block_retention == hours(336));
    check!(provider.expires_any_blocks());
}
