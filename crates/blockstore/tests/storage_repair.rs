//! The report-first storage repair.
//!
//! The repair must refuse a scope that is not explicit, only plan without
//! `apply`, delete only old orphans of the one tenant it names, leave an
//! object that a writer touched after the audit, keep going past a delete
//! that fails, and log every action. After it runs, the lifecycle sweep and
//! the index loaders must see the same live set as before.

mod storage_fixtures;

use std::{
    collections::BTreeSet,
    fmt,
    io::{self, Write},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use assert2::{assert, check};
use futures::{StreamExt as _, stream::BoxStream};
use krabka_blockstore::{
    BlockDescriptor, BlockKey, DEFAULT_BLOCK_SWEEP_GRACE, LabelIndex, LogBlockIndex, ProfileIndex,
    RepairAction, RepairLogEntry, RepairLogPhase, RepairLogWriter, RepairOptions, RepairOutcome,
    RepairReport, StorageAuditError, StorageAuditOptions, StorageFindingKind, StorageSignal,
    TimeRange, TraceIndex, audit_store, index_snapshot_prefix_for_key, labels,
    log_tenant_index_shard_manifest_object_path, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store, reconcile_orphans, repair_store,
    write_log_index_manifest_to_object_store, write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shard_to_object_store,
};
use object_store::{
    GetOptions, GetResult, ObjectStore, ObjectStoreExt as _, PutPayload, path::Path,
};
use storage_fixtures::{
    PROFILE_INDEX, TRACE_INDEX, TenantBlocks, block_key, foreign_entry, healthy, later,
    listed_keys, missing_shard, publish, put_block, put_bytes, sidecar_key, store,
    upper_case_manifest,
};

#[derive(Clone, Debug)]
enum Interference {
    RefuseDelete(String),
    RewriteOnHead(String),
    DeleteOnHead(String),
}

// An object store that plays a second writer, a racing sweep, or a failing
// backend for one named object. The second writer uploads the same bytes
// again on each head request, as a retried upload does. The block stays
// readable, and its entity tag moves.
#[derive(Debug)]
struct InterferingStore {
    inner: Arc<dyn ObjectStore>,
    interference: Mutex<Option<Interference>>,
}

impl fmt::Display for InterferingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

krabka_blockstore::delegate_object_store! {
    InterferingStore => inner;
    forward [put_opts, put_multipart_opts, list, list_with_delimiter, copy_opts];

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let interference = self.interference.lock().unwrap().clone();
        match interference {
            Some(Interference::RewriteOnHead(key)) if options.head && key == location.as_ref() => {
                let bytes = self.inner.get(location).await?.bytes().await?;
                self.inner.put(location, PutPayload::from(bytes)).await?;
            }
            Some(Interference::DeleteOnHead(key)) if options.head && key == location.as_ref() => {
                self.inner.delete(location).await?;
            }
            _ => {}
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let refuse = match &*self.interference.lock().unwrap() {
            Some(Interference::RefuseDelete(key)) => Some(key.clone()),
            _ => None,
        };
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let refuse = refuse.clone();
                async move {
                    let location = location?;
                    if refuse.as_deref() == Some(location.as_ref()) {
                        return Err(object_store::Error::Generic {
                            store: "test",
                            source: "this object refuses to be deleted".into(),
                        });
                    }
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }
}

struct InterferingStoreHandle {
    store: Arc<dyn ObjectStore>,
    handle: Arc<InterferingStore>,
}

fn interfering(inner: &Arc<dyn ObjectStore>, interference: Interference) -> InterferingStoreHandle {
    let store = Arc::new(InterferingStore {
        inner: Arc::clone(inner),
        interference: Mutex::new(Some(interference)),
    });
    InterferingStoreHandle {
        store: Arc::clone(&store) as Arc<dyn ObjectStore>,
        handle: store,
    }
}

// A log that takes `lines` lines and refuses every line after them. The
// repair writes each line with one call.
struct FailingLog {
    lines: Vec<u8>,
    left: usize,
}

impl Write for FailingLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.left == 0 {
            return Err(io::Error::other("the disk is full"));
        }
        self.left -= 1;
        self.lines.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl RepairLogWriter for FailingLog {
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn entries(log: &[u8]) -> Vec<RepairLogEntry> {
    String::from_utf8(log.to_vec())
        .unwrap()
        .lines()
        .map(|line| RepairLogEntry::from_json_line(line).unwrap())
        .collect()
}

fn options(tenant: &str, signal: StorageSignal, apply: bool) -> RepairOptions {
    let kinds = BTreeSet::from([
        StorageFindingKind::Orphan,
        StorageFindingKind::OrphanSidecar,
    ]);
    let mut options = RepairOptions::new(tenant, signal, kinds, later());
    options.apply = apply;
    options
}

fn action(kind: StorageFindingKind, path: &str, outcome: RepairOutcome) -> RepairAction {
    RepairAction {
        kind,
        path: path.to_string(),
        outcome,
        detail: None,
    }
}

struct RepairRun {
    report: RepairReport,
    entries: Vec<RepairLogEntry>,
}

async fn repair(store: &Arc<dyn ObjectStore>, options: &RepairOptions) -> RepairRun {
    let mut log = Vec::new();
    let mut report = repair_store(store, options, &mut log).await.unwrap();
    for action in &mut report.actions {
        action.detail = None;
    }
    RepairRun {
        report,
        entries: entries(&log),
    }
}

fn report(
    applied: bool,
    signal: StorageSignal,
    findings_audited: usize,
    actions: Vec<RepairAction>,
) -> RepairReport {
    RepairReport::new(
        applied,
        "t".to_string(),
        signal,
        vec![
            StorageFindingKind::Orphan,
            StorageFindingKind::OrphanSidecar,
        ],
        findings_audited,
        actions,
    )
}

// Tenants `t` and `u` each get one live block and one old orphan block, and
// for profiles one orphan `.symdb` too. Returns the orphans of tenant `t` in
// the order the repair visits them.
async fn damaged(store: &Arc<dyn ObjectStore>, signal: StorageSignal) -> Vec<RepairAction> {
    healthy(store, signal, &["t", "u"]).await;
    let mut orphans = Vec::new();
    for tenant in ["t", "u"] {
        let orphan = block_key(signal, tenant, 10, 19);
        put_block(store, signal, &orphan).await;
        if tenant == "t" {
            orphans.push(action(
                StorageFindingKind::Orphan,
                &orphan,
                RepairOutcome::Deleted,
            ));
        }
        if signal == StorageSignal::Profiles {
            let sidecar = sidecar_key(signal, &block_key(signal, tenant, 20, 29));
            put_bytes(store, &sidecar, b"symbols").await;
            if tenant == "t" {
                orphans.push(action(
                    StorageFindingKind::OrphanSidecar,
                    &sidecar,
                    RepairOutcome::Deleted,
                ));
            }
        }
    }
    orphans.sort_by(|a, b| a.path.cmp(&b.path));
    orphans
}

fn with_outcome(actions: &[RepairAction], outcome: RepairOutcome) -> Vec<RepairAction> {
    actions
        .iter()
        .map(|action| RepairAction {
            outcome,
            ..action.clone()
        })
        .collect()
}

#[tokio::test]
async fn a_repair_without_an_explicit_scope_is_refused() {
    let store = store();
    let every_kind = BTreeSet::from(StorageFindingKind::ALL);
    struct ScopeCase {
        tenant: &'static str,
        kinds: BTreeSet<StorageFindingKind>,
    }
    let cases = [
        ScopeCase {
            tenant: "",
            kinds: BTreeSet::from([StorageFindingKind::Orphan]),
        },
        ScopeCase {
            tenant: "t",
            kinds: BTreeSet::new(),
        },
        ScopeCase {
            tenant: "t",
            kinds: BTreeSet::from([StorageFindingKind::DanglingIndexEntry]),
        },
        ScopeCase {
            tenant: "t",
            kinds: BTreeSet::from([StorageFindingKind::Pending]),
        },
        ScopeCase {
            tenant: "t",
            kinds: every_kind,
        },
    ];
    for case in cases {
        let mut options = RepairOptions::new(
            case.tenant,
            StorageSignal::Metrics,
            case.kinds.clone(),
            later(),
        );
        options.apply = true;
        let mut log = Vec::new();

        let refused = repair_store(&store, &options, &mut log).await;

        assert!(let Err(StorageAuditError::InvalidScope(_)) = refused, "{:?} {:?}", case.tenant, case.kinds);
        check!(log.is_empty());
    }
}

#[tokio::test]
async fn a_plan_deletes_nothing_and_logs_every_planned_action() {
    for signal in StorageSignal::ALL {
        let store = store();
        let orphans = damaged(&store, signal).await;
        let before = listed_keys(&store).await;

        let repaired = repair(&store, &options("t", signal, false)).await;
        let plan = repaired.report;
        let log = repaired.entries;

        let planned = with_outcome(&orphans, RepairOutcome::Planned);
        check!(
            plan == report(false, signal, orphans.len(), planned.clone()),
            "{signal}"
        );
        check!(listed_keys(&store).await == before, "{signal}");
        check!(log.len() == planned.len(), "{signal}");
        for index in 0..log.len() {
            check!(
                !log[index].applied
                    && log[index].tenant == "t"
                    && log[index].signal == signal
                    && log[index].phase == RepairLogPhase::Outcome,
                "{signal}"
            );
        }
    }
}

#[tokio::test]
async fn an_applied_repair_deletes_only_the_tenants_orphans_and_is_idempotent() {
    for signal in StorageSignal::ALL {
        let store = store();
        let orphans = damaged(&store, signal).await;
        let before = listed_keys(&store).await;
        let deleted: BTreeSet<_> = orphans.iter().map(|action| action.path.clone()).collect();
        let options = options("t", signal, true);

        let first = repair(&store, &options).await.report;
        let run = repair(&store, &options).await;
        let second = run.report;
        let log = run.entries;

        check!(
            first == report(true, signal, orphans.len(), orphans.clone()),
            "{signal}"
        );
        check!(second == report(true, signal, 0, Vec::new()), "{signal}");
        let remaining: Vec<_> = before
            .into_iter()
            .filter(|key| !deleted.contains(key))
            .collect();
        check!(listed_keys(&store).await == remaining, "{signal}");
        check!(log.len() == orphans.len() * 2, "{signal}");
        for index in 0..orphans.len() {
            let action = &orphans[index];
            let intent = &log[index * 2];
            let outcome = &log[index * 2 + 1];
            check!(
                intent.phase == RepairLogPhase::Intent
                    && intent.kind == action.kind
                    && intent.path == action.path
                    && intent.outcome.is_none(),
                "{signal}"
            );
            check!(
                outcome.phase == RepairLogPhase::Outcome
                    && outcome.kind == action.kind
                    && outcome.path == action.path
                    && outcome.outcome == Some(action.outcome),
                "{signal}"
            );
        }
    }
}

#[tokio::test]
async fn a_repair_leaves_an_object_inside_the_grace_window() {
    let store = store();
    let orphans = damaged(&store, StorageSignal::Traces).await;
    let mut options = options("t", StorageSignal::Traces, true);
    options.now = SystemTime::now();

    let report_now = repair(&store, &options).await.report;

    check!(report_now == report(true, StorageSignal::Traces, orphans.len(), Vec::new()));
    check!(listed_keys(&store).await.contains(&orphans[0].path));
}

#[tokio::test]
async fn a_repair_leaves_an_object_that_a_writer_rewrote_after_the_audit() {
    let inner = store();
    let orphans = damaged(&inner, StorageSignal::Metrics).await;
    let target = orphans[0].path.clone();
    let interfering = interfering(&inner, Interference::RewriteOnHead(target.clone()));
    let store = &interfering.store;

    let run = repair(store, &options("t", StorageSignal::Metrics, true)).await;
    let skipped = run.report;
    let log = run.entries;

    let expected = with_outcome(&orphans, RepairOutcome::SkippedChanged);
    check!(skipped == report(true, StorageSignal::Metrics, 1, expected));
    check!(log[0].detail.is_some());
    check!(listed_keys(&inner).await.contains(&target));
}

#[tokio::test]
async fn a_failed_delete_is_reported_and_the_next_run_finishes_it() {
    let inner = store();
    let orphans = damaged(&inner, StorageSignal::Profiles).await;
    let refused = orphans[0].path.clone();
    let interfering = interfering(&inner, Interference::RefuseDelete(refused.clone()));
    let store = &interfering.store;
    let handle = &interfering.handle;
    let options = options("t", StorageSignal::Profiles, true);

    let first = repair(store, &options).await.report;
    *handle.interference.lock().unwrap() = None;
    let second = repair(store, &options).await.report;

    let mut partial = orphans.clone();
    partial[0].outcome = RepairOutcome::Failed;
    check!(first == report(true, StorageSignal::Profiles, 2, partial));
    check!(first.has_failures());
    check!(second == report(true, StorageSignal::Profiles, 1, orphans[..1].to_vec()));
    check!(!listed_keys(&inner).await.contains(&refused));
}

#[tokio::test]
async fn an_object_that_another_sweep_deleted_first_counts_as_absent() {
    let inner = store();
    let orphans = damaged(&inner, StorageSignal::Logs).await;
    let target = orphans[0].path.clone();
    let interfering = interfering(&inner, Interference::DeleteOnHead(target.clone()));
    let store = &interfering.store;

    let raced = repair(store, &options("t", StorageSignal::Logs, true))
        .await
        .report;

    let expected = with_outcome(&orphans, RepairOutcome::AlreadyAbsent);
    check!(raced == report(true, StorageSignal::Logs, 1, expected));
    check!(!raced.has_failures());
    check!(!listed_keys(&inner).await.contains(&target));
}

#[tokio::test]
async fn a_repair_that_cannot_log_its_intent_deletes_nothing() {
    let store = store();
    let orphans = damaged(&store, StorageSignal::Traces).await;
    let before = listed_keys(&store).await;
    let mut log = FailingLog {
        lines: Vec::new(),
        left: 0,
    };

    let refused = repair_store(&store, &options("t", StorageSignal::Traces, true), &mut log).await;

    assert!(let Err(StorageAuditError::AuditLog(_)) = refused);
    check!(log.lines.is_empty());
    check!(listed_keys(&store).await == before);
    check!(before.contains(&orphans[0].path));
}

#[tokio::test]
async fn a_delete_whose_outcome_is_not_logged_keeps_its_intent_line() {
    let store = store();
    let orphans = damaged(&store, StorageSignal::Traces).await;
    let mut log = FailingLog {
        lines: Vec::new(),
        left: 1,
    };

    let stopped = repair_store(&store, &options("t", StorageSignal::Traces, true), &mut log).await;

    assert!(let Err(StorageAuditError::AuditLog(_)) = stopped);
    let intent = RepairLogEntry::intent(
        entries(&log.lines)[0].run_started_at_secs,
        "t",
        StorageSignal::Traces,
        StorageFindingKind::Orphan,
        &orphans[0].path,
    );
    check!(entries(&log.lines) == vec![intent]);
    check!(!listed_keys(&store).await.contains(&orphans[0].path));
}

#[derive(Clone, Copy, Debug)]
enum LiveFault {
    MissingShard,
    UpperCaseManifest,
    ForeignIndexEntry(StorageSignal),
}

// A live block that an audit can mistake for an orphan, and the signal of
// the repair that must leave it.
struct LiveFaultOutcome {
    signal: StorageSignal,
    live_block: String,
}

async fn live_fault(store: &Arc<dyn ObjectStore>, fault: LiveFault) -> LiveFaultOutcome {
    match fault {
        LiveFault::MissingShard => {
            healthy(store, StorageSignal::Logs, &["t", "u"]).await;
            LiveFaultOutcome {
                signal: StorageSignal::Logs,
                live_block: missing_shard(store).await.block,
            }
        }
        LiveFault::UpperCaseManifest => {
            healthy(store, StorageSignal::Metrics, &["t", "u"]).await;
            LiveFaultOutcome {
                signal: StorageSignal::Metrics,
                live_block: upper_case_manifest(store).await,
            }
        }
        LiveFault::ForeignIndexEntry(signal) => {
            healthy(store, signal, &["t", "u"]).await;
            LiveFaultOutcome {
                signal,
                live_block: foreign_entry(store, signal).await,
            }
        }
    }
}

#[tokio::test]
async fn a_repair_of_either_tenant_never_deletes_a_block_that_a_reader_can_reach() {
    for fault in [
        LiveFault::MissingShard,
        LiveFault::UpperCaseManifest,
        LiveFault::ForeignIndexEntry(StorageSignal::Traces),
        LiveFault::ForeignIndexEntry(StorageSignal::Profiles),
    ] {
        let store = store();
        let fault_outcome = live_fault(&store, fault).await;
        let signal = fault_outcome.signal;
        let live = fault_outcome.live_block;
        let before = listed_keys(&store).await;

        let mut reports = Vec::new();
        for tenant in ["t", "u"] {
            reports.push(
                repair(&store, &options(tenant, signal, true))
                    .await
                    .report
                    .actions,
            );
        }

        check!(reports == [Vec::new(), Vec::new()], "{fault:?}");
        check!(listed_keys(&store).await == before, "{fault:?}");
        check!(before.contains(&live), "{fault:?}");
    }
}

#[test]
fn a_log_line_of_another_schema_version_is_refused() {
    let entry = RepairLogEntry::new(
        1,
        true,
        "t",
        StorageSignal::Traces,
        &action(
            StorageFindingKind::Orphan,
            "traces/t/x.parquet",
            RepairOutcome::Deleted,
        ),
    );
    let line = serde_json::to_string(&entry).unwrap();
    check!(RepairLogEntry::from_json_line(&line).unwrap() == entry);

    let mut future: serde_json::Value = serde_json::from_str(&line).unwrap();
    future["schema_version"] = serde_json::json!(entry.schema_version + 1);
    let refused = RepairLogEntry::from_json_line(&future.to_string());
    assert!(let Err(StorageAuditError::UnsupportedSchemaVersion { .. }) = refused);
    assert!(let Err(StorageAuditError::InvalidReport(_)) = RepairLogEntry::from_json_line("{"));
}

async fn live_set(store: &Arc<dyn ObjectStore>, signal: StorageSignal) -> BTreeSet<String> {
    let mut live = BTreeSet::new();
    for tenant in ["t", "u"] {
        match signal {
            StorageSignal::Metrics => {
                let block = block_key(signal, tenant, 0, 9);
                live.insert(sidecar_key(signal, &block));
                live.insert(block);
            }
            StorageSignal::Traces => {
                let index = TraceIndex::load_latest_snapshot(store, TRACE_INDEX)
                    .await
                    .unwrap();
                live.extend(
                    index
                        .trace_blocks(tenant)
                        .iter()
                        .map(|block| block.object_key.clone()),
                );
            }
            StorageSignal::Profiles => {
                let index = ProfileIndex::load_latest_snapshot(store, PROFILE_INDEX)
                    .await
                    .unwrap();
                for block in index.all_blocks() {
                    live.insert(sidecar_key(signal, &block.object_key));
                    live.insert(block.object_key);
                }
            }
            StorageSignal::Logs => {
                let indexes = read_tenant_log_index_manifest_from_object_store(
                    store.as_ref(),
                    &Path::default(),
                    tenant,
                )
                .await
                .unwrap();
                live.extend(
                    indexes
                        .blocks
                        .blocks()
                        .iter()
                        .map(|block| block.key.object_key()),
                );
            }
        }
    }
    live
}

fn block_prefix(signal: StorageSignal, tenant: &str) -> String {
    match signal {
        StorageSignal::Metrics => format!("metrics/{tenant}/"),
        StorageSignal::Traces => format!("traces/{tenant}/"),
        StorageSignal::Profiles => format!("blocks/{tenant}/"),
        StorageSignal::Logs => format!("tenant={tenant}/partition="),
    }
}

#[tokio::test]
async fn after_a_repair_the_indexes_load_and_the_lifecycle_sweep_agrees() {
    for signal in StorageSignal::ALL {
        let store = store();
        damaged(&store, signal).await;
        let live_before = live_set(&store, signal).await;
        let mut options = options("t", signal, true);
        repair(&store, &options).await;
        options.tenant = "u".to_string();
        repair(&store, &options).await;

        let live_after = live_set(&store, signal).await;
        let audit = audit_store(&store, &StorageAuditOptions::new(later()))
            .await
            .unwrap();
        let mut under_prefix = BTreeSet::new();
        for tenant in ["t", "u"] {
            let prefix = block_prefix(signal, tenant);
            under_prefix.extend(
                listed_keys(&store)
                    .await
                    .into_iter()
                    .filter(|key| key.starts_with(&prefix)),
            );
            let stats = reconcile_orphans(
                store.as_ref(),
                &prefix,
                &live_after,
                DEFAULT_BLOCK_SWEEP_GRACE,
                later(),
            )
            .await
            .unwrap();
            check!(stats.deleted == 0 && stats.failed == 0, "{signal} {tenant}");
        }

        check!(live_after == live_before, "{signal}");
        check!(audit.findings == Vec::new(), "{signal}");
        check!(under_prefix == live_after, "{signal}");
    }
}

#[derive(Clone, Debug, PartialEq)]
struct LogShardIndexes {
    label_index: LabelIndex,
    block_index: LogBlockIndex,
}

fn log_shard_indexes(first_offset: i64) -> LogShardIndexes {
    let mut label_index = LabelIndex::default();
    let fingerprint = label_index.insert_series(
        "t",
        labels([krabka_blockstore::LabelPair::new("app", "api")]),
    );
    let mut block_index = LogBlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(
            "t",
            0,
            first_offset,
            first_offset + 9,
            TimeRange::new(10, 19).unwrap(),
        ),
        BTreeSet::from([fingerprint]),
    ));
    LogShardIndexes {
        label_index,
        block_index,
    }
}

async fn publish_log_shard(store: &Arc<dyn ObjectStore>, first_offset: i64) -> String {
    let indexes = log_shard_indexes(first_offset);
    let label_index = indexes.label_index;
    let block_index = indexes.block_index;
    let block = block_index.blocks()[0].key.object_key();
    put_block(store, StorageSignal::Logs, &block).await;
    write_tenant_log_index_shard_to_object_store(
        store.as_ref(),
        &Path::default(),
        "t",
        TimeRange::new(10, 19).unwrap(),
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    block
}

fn log_shard_snapshot(file: &str) -> String {
    let key = log_tenant_index_shard_manifest_object_path(
        &Path::default(),
        "t",
        TimeRange::new(10, 19).unwrap(),
    );
    format!("{}/{file}", index_snapshot_prefix_for_key(key.as_ref()))
}

#[tokio::test]
async fn a_log_repair_keeps_the_newest_shard_blocks_and_removes_old_generation_orphans() {
    for layout in [
        "shard-only",
        "catalog",
        "tenant-manifest",
        "global-manifest",
    ] {
        let store = store();
        let orphan = publish_log_shard(&store, 0).await;
        let live = publish_log_shard(&store, 10).await;
        match layout {
            "catalog" => {
                write_tenant_log_index_shard_catalog_to_object_store(
                    store.as_ref(),
                    &Path::default(),
                    "t",
                    &[TimeRange::new(10, 19).unwrap()],
                )
                .await
                .unwrap();
            }
            "tenant-manifest" | "global-manifest" => {
                let full_block = block_key(StorageSignal::Logs, "t", 20, 29);
                put_block(&store, StorageSignal::Logs, &full_block).await;
                if layout == "tenant-manifest" {
                    publish(
                        &store,
                        StorageSignal::Logs,
                        &[TenantBlocks {
                            tenant: "t",
                            blocks: &[full_block],
                        }],
                        &BTreeSet::new(),
                    )
                    .await;
                } else {
                    let indexes = log_shard_indexes(20);
                    let label_index = indexes.label_index;
                    let block_index = indexes.block_index;
                    write_log_index_manifest_to_object_store(
                        store.as_ref(),
                        &Path::default(),
                        &label_index,
                        &block_index,
                    )
                    .await
                    .unwrap();
                }
            }
            _ => {}
        }
        let before = listed_keys(&store).await;
        let options = options("t", StorageSignal::Logs, true);
        let audit = audit_store(&store, &options.audit_options()).await.unwrap();
        check!(audit.objects_unclassified == 0, "{layout}");

        let applied = repair(&store, &options).await.report;

        assert!(
            applied
                == report(
                    true,
                    StorageSignal::Logs,
                    1,
                    vec![action(
                        StorageFindingKind::Orphan,
                        &orphan,
                        RepairOutcome::Deleted
                    )],
                ),
            "{layout}"
        );
        let remaining: Vec<_> = before.into_iter().filter(|key| key != &orphan).collect();
        check!(listed_keys(&store).await == remaining, "{layout}");
        check!(store.head(&Path::from(live)).await.is_ok(), "{layout}");
        let loaded = read_tenant_log_index_shard_from_object_store(
            store.as_ref(),
            &Path::default(),
            "t",
            TimeRange::new(10, 19).unwrap(),
        )
        .await
        .unwrap();
        let expected = log_shard_indexes(10);
        check!(
            loaded.labels == expected.label_index && loaded.blocks == expected.block_index,
            "{layout}"
        );
    }
}

#[tokio::test]
async fn an_unreadable_newest_log_shard_suppresses_orphan_repair() {
    struct UnreadableShardCase {
        file: &'static str,
        bytes: &'static [u8],
        kind: StorageFindingKind,
    }
    for case in [
        UnreadableShardCase {
            file: "00000000000000000001.json",
            bytes: b"not json",
            kind: StorageFindingKind::UnreadableManifest,
        },
        UnreadableShardCase {
            file: "00000000000000000001.json",
            bytes: br#"{"format_version":99,"series":[],"blocks":[]}"#,
            kind: StorageFindingKind::UnsupportedFormat,
        },
        UnreadableShardCase {
            file: "invalid.json",
            bytes: b"not json",
            kind: StorageFindingKind::UnreadableManifest,
        },
        UnreadableShardCase {
            file: "invalid.JSON",
            bytes: b"not json",
            kind: StorageFindingKind::UnreadableManifest,
        },
        UnreadableShardCase {
            file: "invalid/nested.json",
            bytes: b"not json",
            kind: StorageFindingKind::UnreadableManifest,
        },
    ] {
        let store = store();
        publish_log_shard(&store, 0).await;
        publish_log_shard(&store, 10).await;
        // A valid full manifest does not override an unreadable shard.
        publish(
            &store,
            StorageSignal::Logs,
            &[TenantBlocks {
                tenant: "t",
                blocks: &[],
            }],
            &BTreeSet::new(),
        )
        .await;
        put_bytes(&store, &log_shard_snapshot(case.file), case.bytes).await;
        let before = listed_keys(&store).await;
        let options = options("t", StorageSignal::Logs, true);
        let audit = audit_store(&store, &options.audit_options()).await.unwrap();
        assert!(audit.objects_unclassified == 0, "{}", case.file);
        let findings = audit.findings.iter().all(|finding| {
            finding.kind == case.kind && finding.path == log_shard_snapshot(case.file)
        });
        assert!(findings, "{}", case.file);

        let applied = repair(&store, &options).await.report;

        assert!(
            applied == report(true, StorageSignal::Logs, 1, Vec::new()),
            "{}",
            case.file
        );
        check!(listed_keys(&store).await == before, "{}", case.file);
    }
}

#[tokio::test]
async fn an_unreadable_log_shard_range_suppresses_orphan_repair() {
    for range in ["20-10", "020-029"] {
        let store = store();
        publish_log_shard(&store, 0).await;
        publish_log_shard(&store, 10).await;
        publish(
            &store,
            StorageSignal::Logs,
            &[TenantBlocks {
                tenant: "t",
                blocks: &[],
            }],
            &BTreeSet::new(),
        )
        .await;
        let shard = format!(
            "tenant=t/index/logs/shards/time={range}/manifest/snapshots/00000000000000000000.json"
        );
        put_bytes(&store, &shard, b"not json").await;
        let before = listed_keys(&store).await;
        let options = options("t", StorageSignal::Logs, true);
        let audit = audit_store(&store, &options.audit_options()).await.unwrap();
        assert!(audit.objects_unclassified == 0, "{range}");
        let findings: Vec<_> = audit
            .findings
            .iter()
            .map(|finding| {
                finding.kind == StorageFindingKind::UnreadableManifest && finding.path == shard
            })
            .collect();
        assert!(findings.len() == 1 && findings[0], "{range}");

        let applied = repair(&store, &options).await.report;

        assert!(
            applied == report(true, StorageSignal::Logs, 1, Vec::new()),
            "{range}"
        );
        check!(listed_keys(&store).await == before, "{range}");
        let loaded = read_tenant_log_index_shard_from_object_store(
            store.as_ref(),
            &Path::default(),
            "t",
            TimeRange::new(10, 19).unwrap(),
        )
        .await
        .unwrap();
        let expected = log_shard_indexes(10);
        check!(
            loaded.labels == expected.label_index && loaded.blocks == expected.block_index,
            "{range}"
        );
    }
}
