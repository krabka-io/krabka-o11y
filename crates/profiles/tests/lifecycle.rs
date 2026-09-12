//! What the compactor does to a block after it is written.
//!
//! Every test here drives the real pass over an in-memory object store, and
//! reads the object store back rather than the report: a pass that says it
//! deleted a block and left the object is the failure these exist to catch.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use krabka_blockstore::{
    BlockDeletionReport, BlockIndex as _, BlockLevel, BlockMeta, BlockTimestampUnit,
    CompactionPolicy, DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain, Labels, ObjectStoreMetrics,
    ProfileIndex,
};
use krabka_pprof::{EngineOpts, FlameEngine};
use krabka_profiles::{
    ProfileRecord, WalFunction, WalLocation, WalSample, WalSymbolSet,
    blockbuilder::{BLOCK_OBJECT_PREFIX, STACKTRACE_PARTITION, build_block},
    cold_store::ColdProfileStore,
    lifecycle::{LifecycleOptions, live_object_keys, run_lifecycle_pass, symdb_key},
    limits::{Limits, OverridesProvider},
};
use krabka_units::{Time, hours};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const INDEX_KEY: &str = "index/profiles.json";
/// An epoch-millisecond "now" the tests measure their blocks back from.
const NOW_MS: i64 = 1_700_000_000_000;
const HOUR_MS: i64 = 60 * 60 * 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
/// The grace the orphan sweep runs with here, matching the shipped default.
const GRACE: Time = hours(1);

/// A policy that plans no merge at all: every block already holds the one row
/// the target allows, so the planner skips it. Retention tests then observe
/// retention alone.
fn no_merges() -> CompactionPolicy {
    CompactionPolicy::new(2, 1, BlockLevel(4), hours(2), BlockTimestampUnit::Millis)
}

/// A policy that merges every level-zero block sharing a two-hour bucket.
fn merge_pairs() -> CompactionPolicy {
    CompactionPolicy::new(
        2,
        usize::MAX,
        BlockLevel(4),
        hours(2),
        BlockTimestampUnit::Millis,
    )
}

fn at(now_ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(u64::try_from(now_ms).expect("a positive epoch"))
}

fn retention(entries: &[(&str, i64)]) -> OverridesProvider {
    if entries.is_empty() {
        return OverridesProvider::new(Limits::default());
    }
    let tenants: Vec<String> = entries
        .iter()
        .map(|(tenant, window_secs)| {
            format!("  {tenant}:\n    compactor_blocks_retention_period_secs: {window_secs}\n")
        })
        .collect();
    OverridesProvider::from_yaml(&format!("overrides:\n{}", tenants.concat()))
        .expect("the retention overrides")
}

fn options(
    policy: CompactionPolicy,
    windows: &OverridesProvider,
    now: SystemTime,
) -> LifecycleOptions<'_> {
    LifecycleOptions {
        index_key: INDEX_KEY,
        index_snapshot_retain: IndexSnapshotRetain::default(),
        policy,
        downsample: None,
        retention: windows,
        block_prefix: BLOCK_OBJECT_PREFIX,
        orphan_grace: GRACE,
        now,
    }
}

fn record(tenant: &str, function: &str, timestamp_ms: i64) -> ProfileRecord {
    ProfileRecord {
        tenant: tenant.to_string(),
        labels: vec![
            ("__name__".to_string(), "process_cpu".to_string()),
            ("__profile_type__".to_string(), PT.to_string()),
            ("service_name".to_string(), "api".to_string()),
        ],
        profile_type: PT.to_string(),
        samples: vec![WalSample {
            stacktrace_location_refs: vec![0],
            value: 1,
            // A WAL sample is stamped in nanoseconds and a block's bounds are
            // epoch milliseconds, so the block builder divides on the way in.
            timestamp_ns: timestamp_ms * NANOS_PER_MILLI,
            span_id: None,
            trace_id: None,
        }],
        symbols: WalSymbolSet {
            strings: vec![String::new(), function.to_string()],
            functions: vec![WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![WalLocation {
                address: 0,
                mapping_id: 0,
                lines: vec![(0, 1)],
            }],
            mappings: Vec::new(),
        },
    }
}

/// Writes one block, its symbol database and its index entry, the way the
/// block builder does.
async fn write_block(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    tenant: &str,
    function: &str,
    timestamp_ms: i64,
    offset: i64,
) -> BlockMeta {
    let record = record(tenant, function, timestamp_ms);
    let labels = Labels::from_pairs(record.labels.iter().cloned());
    index
        .add_series(tenant, labels.fingerprint(), &labels)
        .expect("the series registers");
    let meta = build_block(
        store,
        tenant,
        0,
        std::slice::from_ref(&record),
        (offset, offset),
        &ObjectStoreMetrics::unregistered(),
    )
    .await
    .expect("the block builds")
    .remove(0);
    index.add_block(&meta);
    index.add_profile_block(tenant, &meta.object_key, vec![STACKTRACE_PARTITION]);
    meta
}

/// Publishes the index, the way the block builder does after a flush.
///
/// A pass that drops a block needs this: the removal is pinned to the record
/// the published snapshot carries, and a removal of a block no snapshot ever
/// named is rejected rather than applied.
async fn publish(store: &Arc<dyn ObjectStore>, index: &ProfileIndex) {
    index
        .save_latest_snapshot_with_retain(store, INDEX_KEY, IndexSnapshotRetain::default())
        .await
        .expect("the snapshot publishes");
}

async fn exists(store: &Arc<dyn ObjectStore>, key: &str) -> bool {
    store.head(&Path::from(key)).await.is_ok()
}

/// Both the block and the symbol database beside it.
async fn block_objects_exist(store: &Arc<dyn ObjectStore>, key: &str) -> (bool, bool) {
    (
        exists(store, key).await,
        exists(store, &symdb_key(key)).await,
    )
}

fn block_keys(index: &ProfileIndex) -> Vec<String> {
    index
        .compaction_candidates()
        .into_iter()
        .map(|candidate| candidate.object_key)
        .collect()
}

async fn reload(store: &Arc<dyn ObjectStore>) -> ProfileIndex {
    ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
        store,
        INDEX_KEY,
        DEFAULT_INDEX_SNAPSHOT_MAX,
    )
    .await
    .expect("the published snapshot reloads")
}

/// One tenant, one block outside its window and one inside it, with the index
/// already published so the pass has a snapshot to merge against.
async fn one_expired_and_one_live(
    store: &Arc<dyn ObjectStore>,
) -> (ProfileIndex, BlockMeta, BlockMeta) {
    let mut index = ProfileIndex::new();
    let expired = write_block(store, &mut index, "t", "ancient", NOW_MS - 5 * HOUR_MS, 0).await;
    let live = write_block(store, &mut index, "t", "recent", NOW_MS - HOUR_MS / 6, 1).await;
    publish(store, &index).await;
    (index, expired, live)
}

/// A block past its tenant's window loses its index entry, its object and its
/// symbol database. A block inside the window keeps all three.
#[tokio::test]
async fn an_expired_block_and_its_symbol_database_leave_the_bucket() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (mut index, expired, live) = one_expired_and_one_live(&store).await;
    let windows = retention(&[("t", 3600)]);

    let report = run_lifecycle_pass(
        &store,
        &mut index,
        &options(no_merges(), &windows, at(NOW_MS)),
    )
    .await
    .expect("the pass runs");

    check!(report.expired == 1);
    check!(report.deletions.blocks_deleted == 1);
    check!(report.deletions.sidecars_deleted == 1);
    check!(report.deletions.failures.is_empty());
    check!(block_keys(&index) == vec![live.object_key.clone()]);
    check!(block_objects_exist(&store, &expired.object_key).await == (false, false));
    check!(block_objects_exist(&store, &live.object_key).await == (true, true));
}

/// The expired block must not come back when the pass publishes the snapshot
/// that no longer names it. The save merges against the generation that does,
/// so a removal the merge did not pin would be unioned straight back in.
#[tokio::test]
async fn an_expired_block_is_not_resurrected_by_the_snapshot_merge() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (mut index, expired, live) = one_expired_and_one_live(&store).await;
    let windows = retention(&[("t", 3600)]);

    run_lifecycle_pass(
        &store,
        &mut index,
        &options(no_merges(), &windows, at(NOW_MS)),
    )
    .await
    .expect("the pass runs");
    let reloaded = reload(&store).await;

    check!(block_keys(&reloaded) == vec![live.object_key.clone()]);
    check!(
        reloaded
            .stacktrace_partitions(&expired.object_key)
            .is_empty(),
        "the expired block's stacktrace partitions go with it"
    );
}

/// Each tenant is swept by its own window, from the one provider the process
/// resolves every other limit through.
#[tokio::test]
async fn two_tenants_are_swept_by_their_own_windows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    let short = write_block(&store, &mut index, "short", "a", NOW_MS - 5 * HOUR_MS, 0).await;
    let long = write_block(&store, &mut index, "long", "b", NOW_MS - 5 * HOUR_MS, 1).await;
    let unlisted = write_block(&store, &mut index, "unlisted", "c", NOW_MS - 5 * HOUR_MS, 2).await;
    publish(&store, &index).await;
    let windows = retention(&[("short", 3600), ("long", 10 * 3600)]);

    let report = run_lifecycle_pass(
        &store,
        &mut index,
        &options(no_merges(), &windows, at(NOW_MS)),
    )
    .await
    .expect("the pass runs");

    check!(report.expired == 1);
    check!(block_objects_exist(&store, &short.object_key).await == (false, false));
    check!(block_objects_exist(&store, &long.object_key).await == (true, true));
    check!(
        block_objects_exist(&store, &unlisted.object_key).await == (true, true),
        "a tenant with no window configured keeps its blocks forever"
    );
}

/// A merge leaves nothing of its inputs behind, and the block it wrote answers
/// what they answered.
#[tokio::test]
async fn a_merge_deletes_its_inputs_and_their_symbol_databases() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    let first = write_block(&store, &mut index, "t", "alpha", NOW_MS, 0).await;
    let second = write_block(&store, &mut index, "t", "bravo", NOW_MS + 1, 1).await;
    publish(&store, &index).await;
    let windows = retention(&[]);

    let report = run_lifecycle_pass(
        &store,
        &mut index,
        &options(merge_pairs(), &windows, SystemTime::now()),
    )
    .await
    .expect("the pass runs");

    assert!(report.compacted.len() == 1);
    let merged = report.compacted[0].object_key.clone();
    check!(block_objects_exist(&store, &first.object_key).await == (false, false));
    check!(block_objects_exist(&store, &second.object_key).await == (false, false));
    check!(block_objects_exist(&store, &merged).await == (true, true));
    check!(block_keys(&index) == vec![merged]);

    let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
    let engine = FlameEngine::new(cold, EngineOpts::default());
    let graph = engine
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .expect("the merged block answers");

    check!(graph.total == 2);
    for name in ["alpha", "bravo"] {
        check!(graph.names.iter().any(|frame| frame == name), "{name}");
    }
}

/// The live set the orphan sweep is given names each block **and its symbol
/// database**. A set of block keys alone would make every symbol database in
/// the bucket an orphan.
#[tokio::test]
async fn the_live_set_names_every_block_and_its_symbol_database() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    let block = write_block(&store, &mut index, "t", "alpha", NOW_MS, 0).await;

    let live = live_object_keys(&index);

    check!(live.contains(&block.object_key));
    check!(
        live.contains(&symdb_key(&block.object_key)),
        "the symbol database is named by nothing else"
    );
    check!(live.len() == 2);
}

/// The orphan sweep deletes what the index does not name, keeps what it does,
/// keeps what is too new to judge, and **keeps every live symbol database**.
#[tokio::test]
async fn the_orphan_sweep_reclaims_only_unreferenced_and_settled_objects() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    let block = write_block(&store, &mut index, "t", "alpha", NOW_MS, 0).await;
    let orphan = format!("{BLOCK_OBJECT_PREFIX}/t/00000/abandoned.parquet");
    store
        .put(&Path::from(orphan.as_str()), PutPayload::from_static(b"x"))
        .await
        .expect("the orphan is written");
    let windows = retention(&[]);

    // Every object is newer than `now - grace`, so nothing is old enough to
    // judge: a block a writer has put and not yet published looks exactly like
    // this.
    let within_grace = run_lifecycle_pass(
        &store,
        &mut index,
        &options(no_merges(), &windows, SystemTime::now()),
    )
    .await
    .expect("the pass runs");

    check!(within_grace.orphans.deleted == 0);
    check!(within_grace.orphans.kept_within_grace == 1);
    check!(exists(&store, &orphan).await);

    // The same objects, judged from two hours later with a one-hour grace.
    let settled = run_lifecycle_pass(
        &store,
        &mut index,
        &options(
            no_merges(),
            &windows,
            SystemTime::now() + Duration::from_hours(2),
        ),
    )
    .await
    .expect("the pass runs");

    check!(settled.orphans.deleted == 1);
    check!(
        settled.orphans.live == 2,
        "the block and its symbol database"
    );
    check!(!exists(&store, &orphan).await);
    check!(
        block_objects_exist(&store, &block.object_key).await == (true, true),
        "the sweep must never take a live block's symbol database"
    );
}

/// An index that names no block stops the sweep. An empty index is what a
/// first start sees, and it is also what a deployment whose snapshot failed to
/// publish sees; the two are indistinguishable, and one of them is a bucket
/// this would empty.
#[tokio::test]
async fn an_index_that_names_no_block_sweeps_nothing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    let stranded = format!("{BLOCK_OBJECT_PREFIX}/t/00000/stranded.parquet");
    store
        .put(
            &Path::from(stranded.as_str()),
            PutPayload::from_static(b"x"),
        )
        .await
        .expect("the object is written");
    let windows = retention(&[]);

    let report = run_lifecycle_pass(
        &store,
        &mut index,
        &options(
            no_merges(),
            &windows,
            SystemTime::now() + Duration::from_hours(2),
        ),
    )
    .await
    .expect("the pass runs");

    check!(report.orphans == krabka_blockstore::OrphanSweepStats::default());
    check!(exists(&store, &stranded).await);
}

/// A pass with nothing to do publishes no snapshot. Saving on every tick burns
/// a generation and evicts the retained history a reader falls back on.
#[tokio::test]
async fn a_pass_that_changes_nothing_publishes_no_snapshot() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = ProfileIndex::new();
    write_block(&store, &mut index, "t", "alpha", NOW_MS, 0).await;
    let windows = retention(&[]);

    let report = run_lifecycle_pass(
        &store,
        &mut index,
        &options(no_merges(), &windows, SystemTime::now()),
    )
    .await
    .expect("the pass runs");

    check!(!report.changed_the_index());
    check!(report.compacted.is_empty());
    check!(report.expired == 0);
    check!(report.deletions == BlockDeletionReport::default());
    check!(
        ProfileIndex::block_count(&reload(&store).await, "t") == 0,
        "no snapshot was published"
    );
}

/// The cutoff is counted in the unit a profile block stamps its bounds in.
///
/// Read as nanoseconds, a one-hour window would put the cutoff a million hours
/// before the blocks and expire nothing, whatever its age. This pins both
/// sides of one window, so neither answer can be right by accident.
#[tokio::test]
async fn the_retention_cutoff_is_counted_in_milliseconds() {
    for (name, age_ms, want_expired) in [
        ("half an hour old", HOUR_MS / 2, false),
        ("two hours old", 2 * HOUR_MS, true),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut index = ProfileIndex::new();
        let block = write_block(&store, &mut index, "t", "alpha", NOW_MS - age_ms, 0).await;
        publish(&store, &index).await;
        let windows = retention(&[("t", 3600)]);

        let report = run_lifecycle_pass(
            &store,
            &mut index,
            &options(no_merges(), &windows, at(NOW_MS)),
        )
        .await
        .expect("the pass runs");

        check!((report.expired == 1) == want_expired, "{name}");
        let want_objects = (!want_expired, !want_expired);
        check!(
            block_objects_exist(&store, &block.object_key).await == want_objects,
            "{name}"
        );
    }
}
