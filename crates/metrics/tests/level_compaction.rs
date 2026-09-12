//! Level compaction of metric blocks that are already in object storage: what
//! merges, what never does, and the order a pass applies its result in.

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use krabka_blockstore::{
    BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX, Labels,
    read_block,
};
use krabka_metrics::{
    BucketSpan, CompactionIndexManifest, CompactionObjectPlan, CompactionSeriesLabels,
    DeferredBlockDeletions, ExemplarRow, FloatRow, MetricBlockKind, MetricCompactionPass,
    NativeHistogram, NativeHistogramRow, ObjectStoreCompactionIndexSink, ResetHint,
    TenantCompactionRows, compact_metric_blocks_once, decode_float_samples,
    decode_native_histograms, enforce_compaction_retention, list_compaction_manifests,
    plan_metric_compactions, write_compacted_tenant_blocks,
};
use krabka_units::{Time, hours, secs};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

/// A wall-clock instant well inside the range every unit can express.
const NOW_MS: i64 = 1_700_000_000_000;

fn labels(name: &str) -> Labels {
    Labels::from_pairs([("__name__", name)])
}

/// A policy whose window is wide enough to hold every sample a test writes, so
/// only the rule under test decides what merges.
fn policy(max_blocks_per_job: usize, target_rows: usize, max_level: u32) -> CompactionPolicy {
    CompactionPolicy::new(
        max_blocks_per_job,
        target_rows,
        BlockLevel(max_level),
        hours(24),
        BlockTimestampUnit::Millis,
    )
}

fn rows(tenant: &str, samples: &[(u64, i64, f64)]) -> TenantCompactionRows {
    TenantCompactionRows {
        tenant: tenant.to_string(),
        series_labels: samples
            .iter()
            .map(|(fingerprint, _, _)| (*fingerprint, labels(&format!("series_{fingerprint}"))))
            .collect(),
        float_rows: samples
            .iter()
            .map(|(fingerprint, timestamp_ms, value)| FloatRow {
                fingerprint: *fingerprint,
                timestamp_ms: *timestamp_ms,
                value: *value,
            })
            .collect(),
        histogram_rows: Vec::new(),
        exemplar_rows: Vec::new(),
        metadata_rows: Vec::new(),
        clock_rows: Vec::new(),
    }
}

/// Writes one level-zero float block the way the block builder writes it, and
/// answers with the manifest a pass will read.
async fn write_float_block(
    store: &Arc<dyn ObjectStore>,
    tenant: &str,
    first_offset: i64,
    samples: &[(u64, i64, f64)],
) -> CompactionIndexManifest {
    let mut writes = write_compacted_tenant_blocks(
        &BlockWriter::new(store.clone()),
        &ObjectStoreCompactionIndexSink::new(store.clone()),
        &rows(tenant, samples),
        first_offset,
        first_offset + 1,
    )
    .await
    .expect("write a level-zero float block");
    assert!(writes.len() == 1);
    writes.remove(0).manifest
}

async fn run_pass(
    store: &Arc<dyn ObjectStore>,
    policy: CompactionPolicy,
    deferred: &mut DeferredBlockDeletions,
) -> MetricCompactionPass {
    compact_metric_blocks_once(
        store,
        &BlockWriter::new(store.clone()),
        &ObjectStoreCompactionIndexSink::new(store.clone()),
        policy,
        DEFAULT_BLOCK_READ_MAX,
        deferred,
    )
    .await
    .expect("one compaction pass")
}

async fn exists(store: &Arc<dyn ObjectStore>, key: &str) -> bool {
    store.head(&Path::from(key)).await.is_ok()
}

/// Every `(fingerprint, timestamp, value)` the block at `key` holds.
async fn samples_in(store: &Arc<dyn ObjectStore>, key: &str) -> Vec<(u64, i64, f64)> {
    read_block(store.clone(), key)
        .await
        .expect("read the block")
        .iter()
        .flat_map(|batch| decode_float_samples(batch).expect("decode float samples"))
        .collect()
}

/// A hand-built manifest, for the tests that ask the planner a question and
/// never read a block.
fn manifest(
    tenant: &str,
    kind: MetricBlockKind,
    block_key: &str,
    level: BlockLevel,
    (min_ts, max_ts): (i64, i64),
    row_count: usize,
) -> CompactionIndexManifest {
    CompactionIndexManifest {
        tenant: tenant.to_string(),
        kind,
        block_key: block_key.to_string(),
        index_key: format!("{block_key}.index"),
        level,
        first_offset: 0,
        last_offset: 1,
        row_count,
        min_ts,
        max_ts,
        fingerprints: vec![7],
        series: Vec::new(),
    }
}

#[tokio::test]
async fn two_level_zero_blocks_in_one_window_become_one_level_one_block() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = write_float_block(
        &store,
        "tenant-a",
        1,
        &[(7, NOW_MS, 1.0), (7, NOW_MS + 1_000, 2.0)],
    )
    .await;
    let second = write_float_block(
        &store,
        "tenant-a",
        3,
        &[(7, NOW_MS + 2_000, 3.0), (9, NOW_MS + 500, 4.0)],
    )
    .await;
    let mut deferred = DeferredBlockDeletions::new();

    let pass = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

    assert!(pass.outputs.len() == 1);
    let output = &pass.outputs[0];
    check!(output.level == BlockLevel(1));
    check!(output.kind == MetricBlockKind::Float);
    check!(output.tenant == "tenant-a");
    check!(output.row_count == 4, "every input row, none of them twice");
    check!(output.min_ts == NOW_MS);
    check!(output.max_ts == NOW_MS + 2_000);
    check!(output.fingerprints == vec![7, 9]);
    // The label sets of both inputs, or a matcher would reach no series in the
    // merged block.
    check!(
        output.series
            == vec![
                CompactionSeriesLabels {
                    fingerprint: 7,
                    labels: labels("series_7"),
                },
                CompactionSeriesLabels {
                    fingerprint: 9,
                    labels: labels("series_9"),
                },
            ]
    );

    check!(
        samples_in(&store, &output.block_key).await
            == vec![
                (7, NOW_MS, 1.0),
                (7, NOW_MS + 1_000, 2.0),
                (7, NOW_MS + 2_000, 3.0),
                (9, NOW_MS + 500, 4.0),
            ],
        "every sample is still there, in the declared order"
    );
    // The index entries of the inputs are gone, so a listing now names the
    // merged block alone.
    let live = list_compaction_manifests(&store)
        .await
        .expect("list manifests");
    check!(live == vec![output.clone()]);
    check!(!exists(&store, &first.index_key).await);
    check!(!exists(&store, &second.index_key).await);
}

/// A block builder that crashed before it committed re-emits the same records
/// under a different key, so two blocks can hold the same rows. The `PromQL`
/// engine drops one of the pair at query time; a merge that concatenated them
/// would bake both into one block and inflate the `row_count` that the planner
/// reads and that a user sees as `num_samples`.
#[tokio::test]
async fn duplicate_samples_across_two_inputs_appear_once_in_the_merged_block() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let overlap = [(7, NOW_MS, 1.0), (7, NOW_MS + 1_000, 2.0)];
    write_float_block(&store, "tenant-a", 1, &overlap).await;
    write_float_block(&store, "tenant-a", 3, &overlap).await;
    let mut deferred = DeferredBlockDeletions::new();

    let pass = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

    assert!(pass.outputs.len() == 1);
    let output = &pass.outputs[0];
    check!(output.row_count == 2, "two rows in, two rows out, not four");
    check!(samples_in(&store, &output.block_key).await == overlap.to_vec());
}

/// The termination property. Left alone with no new ingest, repeated
/// plan-and-apply reaches an empty plan: a job needs two inputs below the
/// ladder's cap, and each round lifts its output one rung. A pass that read its
/// own output back as level zero would merge for as long as the process ran.
#[tokio::test]
async fn repeated_planning_reaches_an_empty_plan() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for offset in 0..8 {
        write_float_block(
            &store,
            "tenant-a",
            offset * 2,
            &[(7, NOW_MS + offset * 1_000, 1.0)],
        )
        .await;
    }
    let policy = policy(2, 1_000_000, 3);
    let mut deferred = DeferredBlockDeletions::new();

    let mut passes = 0;
    loop {
        let pass = run_pass(&store, policy, &mut deferred).await;
        if pass.outputs.is_empty() {
            break;
        }
        passes += 1;
        assert!(passes < 20, "a plan-and-apply loop that does not terminate");
    }

    let live = list_compaction_manifests(&store)
        .await
        .expect("list manifests");
    // Eight blocks, a fan-in of two and a ladder of three rungs: every block
    // reaches level three, where it is never an input again.
    check!(live.iter().all(|manifest| manifest.level == BlockLevel(3)));
    check!(
        plan_metric_compactions(&live, policy).is_empty(),
        "the plan is empty, and not merely applied"
    );
    // Every sample survived the whole ladder.
    let mut merged = Vec::new();
    for manifest in &live {
        merged.extend(samples_in(&store, &manifest.block_key).await);
    }
    merged.sort_by_key(|(_, timestamp, _)| *timestamp);
    check!(
        merged
            == (0..8)
                .map(|offset| (7, NOW_MS + offset * 1_000, 1.0))
                .collect::<Vec<_>>()
    );
}

/// A job never spans two windows. Merging across them would produce a block
/// whose range covers data it does not hold, and every query inside the gap
/// would open it for nothing.
#[test]
fn blocks_in_different_windows_are_not_merged_into_one_job() {
    let window = hours(2);
    let window_ms = BlockTimestampUnit::Millis.ticks(window);
    let manifests = vec![
        manifest(
            "tenant-a",
            MetricBlockKind::Float,
            "early-a",
            BlockLevel(0),
            (0, 1_000),
            1,
        ),
        manifest(
            "tenant-a",
            MetricBlockKind::Float,
            "early-b",
            BlockLevel(0),
            (2_000, 3_000),
            1,
        ),
        manifest(
            "tenant-a",
            MetricBlockKind::Float,
            "late-a",
            BlockLevel(0),
            (window_ms, window_ms + 1_000),
            1,
        ),
        manifest(
            "tenant-a",
            MetricBlockKind::Float,
            "late-b",
            BlockLevel(0),
            (window_ms + 2_000, window_ms + 3_000),
            1,
        ),
    ];

    let jobs = plan_metric_compactions(
        &manifests,
        CompactionPolicy::new(
            8,
            1_000_000,
            BlockLevel(4),
            window,
            BlockTimestampUnit::Millis,
        ),
    );

    check!(
        jobs.iter()
            .map(|planned| planned.job.input_keys.clone())
            .collect::<Vec<_>>()
            == vec![
                vec!["early-a".to_string(), "early-b".to_string()],
                vec!["late-a".to_string(), "late-b".to_string()],
            ]
    );
}

/// Only the two kinds whose rows are keyed by `(fingerprint, timestamp)` are
/// ever offered to the planner. See `MetricBlockKind::is_mergeable` for why each
/// of the other three is left out: exemplars share a key legitimately, metadata
/// rows have no real timestamp, and nothing reads a clock block on any query
/// path.
#[test]
fn only_float_and_native_histogram_blocks_are_ever_compaction_inputs() {
    for (kind, mergeable) in [
        (MetricBlockKind::Float, true),
        (MetricBlockKind::NativeHistograms, true),
        (MetricBlockKind::Exemplars, false),
        (MetricBlockKind::Metadata, false),
        (MetricBlockKind::ClockReadings, false),
    ] {
        let manifests = vec![
            manifest("tenant-a", kind, "first", BlockLevel(0), (0, 1_000), 1),
            manifest("tenant-a", kind, "second", BlockLevel(0), (1, 1_001), 1),
        ];

        let jobs = plan_metric_compactions(&manifests, policy(8, 1_000_000, 4));

        check!(!jobs.is_empty() == mergeable, "{kind:?}");
    }
}

/// The exclusion has to hold over a real bucket too, and not only over the
/// planner: an exemplar block beside a float block must come out of a pass
/// exactly as it went in.
#[tokio::test]
async fn a_pass_leaves_exemplar_blocks_alone_while_it_merges_float_blocks() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let sink = ObjectStoreCompactionIndexSink::new(store.clone());
    let block_writer = BlockWriter::new(store.clone());
    let mut exemplars = Vec::new();
    for offset in [1_i64, 3] {
        let mut rows = rows("tenant-a", &[(7, NOW_MS + offset, 1.0)]);
        // Two exemplars on one `(fingerprint, timestamp)`, which is exactly the
        // pair a merge would have deduplicated away.
        rows.exemplar_rows = vec![
            ExemplarRow {
                fingerprint: 7,
                timestamp_ms: NOW_MS,
                value: 1.0,
                trace_id: Some("trace-one".to_string()),
                span_id: None,
                labels: Vec::new(),
            },
            ExemplarRow {
                fingerprint: 7,
                timestamp_ms: NOW_MS,
                value: 2.0,
                trace_id: Some("trace-two".to_string()),
                span_id: None,
                labels: Vec::new(),
            },
        ];
        let writes = write_compacted_tenant_blocks(&block_writer, &sink, &rows, offset, offset + 1)
            .await
            .expect("write a float block and an exemplar block");
        exemplars.extend(
            writes
                .into_iter()
                .map(|write| write.manifest)
                .filter(|manifest| manifest.kind == MetricBlockKind::Exemplars),
        );
    }
    assert!(exemplars.len() == 2);
    let mut deferred = DeferredBlockDeletions::new();

    let pass = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

    check!(
        pass.outputs
            .iter()
            .map(|output| output.kind)
            .collect::<Vec<_>>()
            == vec![MetricBlockKind::Float]
    );
    for exemplar in &exemplars {
        check!(exists(&store, &exemplar.block_key).await, "{exemplar:?}");
        check!(exists(&store, &exemplar.index_key).await);
    }
    check!(
        list_compaction_manifests(&store)
            .await
            .expect("list manifests")
            .iter()
            .filter(|manifest| manifest.kind == MetricBlockKind::Exemplars)
            .count()
            == 2
    );
}

/// `krabka-metrics-service` caches a cold block index, so a querier can hold an
/// index that names a retired input for as long as that cache lives. The pass
/// that retires an input therefore leaves its object alone, and a later pass
/// deletes it.
#[tokio::test]
async fn a_merge_does_not_delete_its_inputs_until_a_later_pass() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = write_float_block(&store, "tenant-a", 1, &[(7, NOW_MS, 1.0)]).await;
    let second = write_float_block(&store, "tenant-a", 3, &[(7, NOW_MS + 1_000, 2.0)]).await;
    let mut deferred = DeferredBlockDeletions::new();

    let merging = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

    assert!(merging.outputs.len() == 1);
    check!(merging.manifests_retired.deleted == 2, "both index entries");
    check!(
        merging.blocks_deleted.deleted == 0,
        "and not one block object"
    );
    check!(
        deferred.keys() == [first.block_key.clone(), second.block_key.clone()],
        "the inputs are queued instead"
    );
    for input in [&first, &second] {
        check!(exists(&store, &input.block_key).await);
    }

    let sweeping = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

    check!(sweeping.outputs.is_empty(), "one output cannot be a job");
    check!(sweeping.blocks_deleted.deleted == 2);
    check!(deferred.is_empty());
    for input in [&first, &second] {
        check!(!exists(&store, &input.block_key).await);
    }
    // The merged block and its manifest are the only things left.
    check!(
        list_compaction_manifests(&store)
            .await
            .expect("list manifests")
            == merging.outputs
    );
}

/// A merged block's key names a level and a range rather than a WAL offset
/// window, and the retention pass reads both shapes: the manifest says what to
/// expire, and the key is only the object to delete. A pass that could not read
/// one of the two shapes would keep those blocks forever.
#[tokio::test]
async fn retention_expires_a_compacted_block_as_readily_as_a_level_zero_one() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    write_float_block(&store, "tenant-a", 1, &[(7, NOW_MS - 10_000, 1.0)]).await;
    write_float_block(&store, "tenant-a", 3, &[(7, NOW_MS - 9_000, 2.0)]).await;
    let mut deferred = DeferredBlockDeletions::new();
    let merged = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;
    assert!(merged.outputs.len() == 1);
    let compacted = merged.outputs[0].clone();
    check!(compacted.block_key.contains("/float/compacted/l1-"));
    // A level-zero block of another tenant, so one pass judges both shapes.
    let ingested = write_float_block(&store, "tenant-b", 5, &[(7, NOW_MS - 10_000, 3.0)]).await;
    // Both shapes are listed as manifests in the first place.
    check!(
        list_compaction_manifests(&store)
            .await
            .expect("list manifests")
            .iter()
            .map(|manifest| manifest.block_key.clone())
            .collect::<Vec<_>>()
            == vec![compacted.block_key.clone(), ingested.block_key.clone()],
        "a listing sorts by key, so the merged block comes first"
    );

    let stats = enforce_compaction_retention(
        &store,
        std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(u64::try_from(NOW_MS).expect("an epoch time")),
        &Windows(secs(5)),
    )
    .await
    .expect("enforce retention");

    check!(stats.manifests_scanned == 2);
    check!(stats.manifests_retired.deleted == 2);
    check!(stats.blocks_deleted.deleted == 2);
    check!(!exists(&store, &compacted.block_key).await);
    check!(!exists(&store, &compacted.index_key).await);
    check!(!exists(&store, &ingested.block_key).await);
    check!(!exists(&store, &ingested.index_key).await);
}

/// One window for every tenant, so a test that cares about the key shape rather
/// than the per-tenant table does not have to name its tenants.
struct Windows(Time);

impl krabka_blockstore::RetentionWindows for Windows {
    fn block_retention(&self, _tenant: &str) -> Time {
        self.0
    }
}

/// The manifest is encoded with a positional binary codec, so a field that the
/// decoder does not expect is not a missing field: it is every field after it
/// read at the wrong offset. This is the test that says the codec still works.
#[test]
fn the_level_survives_a_manifest_round_trip() {
    for level in [BlockLevel(0), BlockLevel(1), BlockLevel(4)] {
        let plan = CompactionObjectPlan {
            block_key: "metrics/tenant-a/float/compacted/l1-0-10-0123456789abcdef.parquet"
                .to_string(),
            index_key: "metrics/tenant-a/float/compacted/l1-0-10-0123456789abcdef.index"
                .to_string(),
            first_offset: 42,
            last_offset: 99,
            row_count: 2,
        };
        let expected = CompactionIndexManifest {
            tenant: "tenant-a".to_string(),
            kind: MetricBlockKind::Float,
            block_key: plan.block_key.clone(),
            index_key: plan.index_key.clone(),
            level,
            first_offset: 42,
            last_offset: 99,
            row_count: 2,
            min_ts: 1_000,
            max_ts: 2_000,
            fingerprints: vec![7],
            series: vec![CompactionSeriesLabels {
                fingerprint: 7,
                labels: labels("up"),
            }],
        };

        let decoded =
            CompactionIndexManifest::decode(&expected.encode().expect("encode a manifest"))
                .expect("decode a manifest");

        assert!(decoded == expected, "{level}");
    }
}

/// The output key names the level, the range, and a fingerprint of its inputs.
/// Two jobs over one range must not agree on a name, or one would overwrite the
/// other's block and the samples of the loser would be gone.
#[tokio::test]
async fn two_merges_over_one_range_write_to_different_keys() {
    let mut keys = BTreeMap::new();
    for tenant in ["tenant-a", "tenant-b"] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        write_float_block(&store, tenant, 1, &[(7, NOW_MS, 1.0)]).await;
        write_float_block(&store, tenant, 3, &[(7, NOW_MS + 1_000, 2.0)]).await;
        let mut deferred = DeferredBlockDeletions::new();

        let pass = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

        assert!(pass.outputs.len() == 1);
        keys.insert(tenant, pass.outputs[0].block_key.clone());
    }

    check!(keys["tenant-a"] != keys["tenant-b"]);
    for (tenant, key) in &keys {
        check!(
            key.starts_with(&format!("metrics/{tenant}/float/compacted/l1-")),
            "{key}"
        );
        check!(key.ends_with(".parquet"), "{key}");
    }
}

/// Native histograms merge as well as floats do, and this is the kind that says
/// so over real blocks: its schema carries four list columns and a struct list,
/// and a merge concatenates all of them.
#[tokio::test]
async fn two_native_histogram_blocks_merge_into_one() {
    let histogram = |count: f64| NativeHistogram {
        schema: 1,
        is_float: false,
        reset_hint: ResetHint::No,
        zero_threshold: 0.5,
        zero_count: 1.0,
        count,
        sum: count * 2.0,
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 1,
        }],
        positive_counts: vec![count],
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: Some(NOW_MS),
    };
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let sink = ObjectStoreCompactionIndexSink::new(store.clone());
    let block_writer = BlockWriter::new(store.clone());
    for (offset, timestamp_ms, count) in [(1_i64, NOW_MS, 3.0), (3, NOW_MS + 1_000, 5.0)] {
        let mut rows = rows("tenant-a", &[]);
        rows.series_labels = BTreeMap::from([(7, labels("latency"))]);
        rows.histogram_rows = vec![NativeHistogramRow {
            fingerprint: 7,
            timestamp_ms,
            hist: histogram(count),
        }];
        write_compacted_tenant_blocks(&block_writer, &sink, &rows, offset, offset + 1)
            .await
            .expect("write a native-histogram block");
    }
    let mut deferred = DeferredBlockDeletions::new();

    let pass = run_pass(&store, policy(8, 1_000_000, 4), &mut deferred).await;

    assert!(pass.outputs.len() == 1);
    let output = &pass.outputs[0];
    check!(output.kind == MetricBlockKind::NativeHistograms);
    check!(output.level == BlockLevel(1));
    check!(output.row_count == 2);
    let decoded: Vec<(u64, i64, NativeHistogram)> = read_block(store.clone(), &output.block_key)
        .await
        .expect("read the merged block")
        .iter()
        .flat_map(|batch| decode_native_histograms(batch).expect("decode native histograms"))
        .collect();
    check!(
        decoded
            == vec![
                (7, NOW_MS, histogram(3.0)),
                (7, NOW_MS + 1_000, histogram(5.0)),
            ]
    );
}
