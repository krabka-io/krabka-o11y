use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::{
    BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX,
};
use krabka_metrics::{
    BucketSpan, DeferredBlockDeletions, NativeHistogram, ObjectStoreCompactionIndexSink, ResetHint,
    WalRecord, compact_metric_blocks_once, compact_wal_records, list_compaction_manifests,
    write_compacted_tenant_blocks,
};
use krabka_units::hours;
use object_store::local::LocalFileSystem;

use super::*;
use crate::{
    InMemoryMetricStore, MergedMetricStore, PromqlString, WalHead, evaluate_recording_rule,
};

/// The cold store over every compaction manifest in `object_store`.
async fn manifest_cold_store(
    object_store: &Arc<dyn ObjectStore>,
    base: &url::Url,
) -> MetricBlockStore {
    let manifests = list_compaction_manifests(object_store).await.unwrap();
    MetricBlockStore::from_compaction_manifests(
        BlockStore::new(object_store.clone(), base.clone()),
        Some(BlockStore::new(object_store.clone(), base.clone())),
        &manifests,
    )
}

struct ReopenedManifestEngine {
    object_store: Arc<dyn ObjectStore>,
    engine: PromqlEngine<MetricBlockStore>,
}

async fn reopen_manifest_engine(
    objects: &std::path::Path,
    base: &url::Url,
) -> ReopenedManifestEngine {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(objects).unwrap());
    let engine = PromqlEngine::new(
        Arc::new(manifest_cold_store(&object_store, base).await),
        EngineOpts::default(),
    );
    ReopenedManifestEngine {
        object_store,
        engine,
    }
}

/// Runs two metric-block compaction passes over `object_store`.
async fn compact_twice(
    object_store: &Arc<dyn ObjectStore>,
    policy: CompactionPolicy,
    deferred: &mut DeferredBlockDeletions,
) {
    for _ in 0..2 {
        compact_metric_blocks_once(
            object_store,
            &BlockWriter::new(object_store.clone()),
            &ObjectStoreCompactionIndexSink::new(object_store.clone()),
            policy,
            DEFAULT_BLOCK_READ_MAX,
            deferred,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn byte_recording_rules_survive_wal_compaction_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = recording_rule_fixture().await;
    let records = fixture.records;
    let values = fixture.values;
    for index in 0..records.len() {
        let record = &records[index];
        std::fs::write(
            directory.path().join(format!("wal-{index}")),
            record.encode().unwrap(),
        )
        .unwrap();
    }
    drop(records);
    let records = (0..8)
        .map(|index| {
            WalRecord::decode(
                &std::fs::read(directory.path().join(format!("wal-{index}"))).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut head = InMemoryMetricStore::new();
    for record in &records {
        head.apply_wal_record(record);
    }
    let hot_engine = PromqlEngine::new(Arc::new(head.clone()), EngineOpts::default());
    check_byte_ledger(&hot_engine, &values).await;
    drop(hot_engine);
    let objects = directory.path().join("objects");
    std::fs::create_dir(&objects).unwrap();
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&objects).unwrap());
    for offset in [0, 8] {
        for rows in compact_wal_records(&records) {
            write_compacted_tenant_blocks(
                &BlockWriter::new(object_store.clone()),
                &ObjectStoreCompactionIndexSink::new(object_store.clone()),
                &rows,
                offset,
                offset + 7,
            )
            .await
            .unwrap();
        }
    }
    let base = url::Url::parse("file:///").unwrap();
    let cold = manifest_cold_store(&object_store, &base).await;
    let overlap_engine = PromqlEngine::new(
        Arc::new(MergedMetricStore::new(cold, WalHead::from_store(head))),
        EngineOpts::default(),
    );
    check_byte_ledger(&overlap_engine, &values).await;
    drop(overlap_engine);
    let mut deferred = DeferredBlockDeletions::new();
    let policy = CompactionPolicy::new(
        8,
        1_000_000,
        BlockLevel(4),
        hours(24),
        BlockTimestampUnit::Millis,
    );
    compact_twice(&object_store, policy, &mut deferred).await;
    drop(object_store);
    // Reopening the filesystem and rebuilding solely from persisted sidecars
    // prevents an in-process label cache from hiding a lossy storage adapter.
    let reopened_engine = reopen_manifest_engine(&objects, &base).await;
    check_byte_ledger(&reopened_engine.engine, &values).await;
    let reopened = reopened_engine.object_store;
    drop(reopened_engine.engine);
    let raw_matcher = |byte| krabka_blockstore::ByteLabelMatcher {
        name: "raw".to_owned(),
        op: krabka_blockstore::MatchOp::Eq,
        value: vec![byte],
    };
    // Both a different tenant and a disjoint closed time window must leave FE
    // intact. The matching request removes only FF across both sample kinds.
    for case in [
        ErasureCase {
            tenant: "tenant-b",
            start: 30_000_000_000,
            end: 30_000_000_000,
            byte: 0xfe,
        },
        ErasureCase {
            tenant: "tenant-a",
            start: 31_000_000_000,
            end: 31_000_000_000,
            byte: 0xfe,
        },
        ErasureCase {
            tenant: "tenant-a",
            start: 30_000_000_000,
            end: 30_000_000_000,
            byte: 0xff,
        },
    ] {
        let request = krabka_blockstore::ErasureRequest::new(
            case.tenant,
            "byte ledger",
            Vec::new(),
            case.start,
            case.end,
            40_000_000_000,
        )
        .with_byte_matchers(vec![vec![raw_matcher(case.byte)]]);
        krabka_blockstore::put_erasure_request(
            &reopened,
            krabka_blockstore::ERASURE_REQUEST_PREFIX,
            &request,
        )
        .await
        .unwrap();
    }
    compact_twice(&reopened, policy, &mut deferred).await;
    drop(reopened);
    let reopened_engine = reopen_manifest_engine(&objects, &base).await;
    let engine = reopened_engine.engine;
    for case in [
        QueryValueCase {
            query: "recorded_float",
            expected: 2.0,
        },
        QueryValueCase {
            query: "histogram_count(recorded_hist)",
            expected: 5.0,
        },
    ] {
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 30_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 3);
        assert2::assert!(
            raw_label_values(&samples)
                == BTreeSet::from([
                    vec![0xfe],
                    "�".as_bytes().to_vec(),
                    b"__krabka_bytes_ff".to_vec()
                ])
        );
        for sample in samples {
            assert2::assert!(sample.value == SampleValue::Float(case.expected));
        }
    }
    let QueryResult::InstantVector(samples) = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"recorded_float{raw="\xff"}"#,
            30_000,
        )
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(samples.is_empty());
}

struct RecordingRuleFixture {
    records: Vec<WalRecord>,
    values: [Vec<u8>; 4],
}

struct ErasureCase {
    tenant: &'static str,
    start: i64,
    end: i64,
    byte: u8,
}

struct QueryValueCase {
    query: &'static str,
    expected: f64,
}

fn raw_label_values(samples: &[crate::InstantSample]) -> BTreeSet<Vec<u8>> {
    samples
        .iter()
        .map(|sample| sample.labels.get_value("raw").unwrap().as_bytes().to_vec())
        .collect()
}

async fn instant_vector<S: MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
) -> Vec<crate::InstantSample> {
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), query, 30_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    samples
}

struct RecordingRuleCase {
    input: &'static str,
    output: &'static str,
}

struct QueryBytesCase {
    query: &'static str,
    expected: Vec<Vec<u8>>,
}

async fn recording_rule_fixture() -> RecordingRuleFixture {
    let mut seed = InMemoryMetricStore::new();
    seed.push_float(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new(
            "__name__",
            "input_float",
        )]),
        30_000,
        2.0,
    );
    seed.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new(
            "__name__",
            "input_hist",
        )]),
        30_000,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::Gauge,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 5.0,
            sum: 5.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![5.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    let seed_engine = PromqlEngine::new(Arc::new(seed), EngineOpts::default());
    let values = [
        vec![0xff],
        vec![0xfe],
        "\u{fffd}".as_bytes().to_vec(),
        b"__krabka_bytes_ff".to_vec(),
    ];
    let mut records = Vec::new();
    for case in [
        RecordingRuleCase {
            input: "input_float",
            output: "recorded_float",
        },
        RecordingRuleCase {
            input: "input_hist",
            output: "recorded_hist",
        },
    ] {
        let query = values
            .iter()
            .map(|bytes| {
                let literal = PromqlString::from(bytes.clone()).quoted();
                format!(
                    "label_replace({},\"raw\",{literal},\"\",\".*\")",
                    case.input
                )
            })
            .collect::<Vec<_>>()
            .join(" or ");
        let materialized = evaluate_recording_rule(
            &seed_engine,
            &tenant_id("tenant-a"),
            case.output,
            &query,
            &BTreeMap::new(),
            30_000,
        )
        .await
        .unwrap();
        assert2::assert!(materialized.len() == 4);
        assert2::assert!(
            materialized
                .iter()
                .map(|record| record
                    .labels()
                    .get_value("raw")
                    .unwrap()
                    .as_bytes()
                    .to_vec())
                .collect::<BTreeSet<_>>()
                == values.iter().cloned().collect::<BTreeSet<_>>()
        );
        // Replacing the distinguishing raw label must still reject true collisions.
        let error = evaluate_recording_rule(
            &seed_engine,
            &tenant_id("tenant-a"),
            case.output,
            &query,
            &{
                let mut labels = BTreeMap::new();
                labels.insert("raw".to_owned(), "collapsed".to_owned());
                labels
            },
            30_000,
        )
        .await
        .unwrap_err();
        assert2::assert!(error.to_string().contains("same labelset"));
        records.extend(materialized);
    }
    RecordingRuleFixture { records, values }
}

async fn check_byte_ledger<S: MetricStore>(engine: &PromqlEngine<S>, values: &[Vec<u8>; 4]) {
    for case in [
        QueryValueCase {
            query: "sum by(raw)(recorded_float)",
            expected: 2.0,
        },
        QueryValueCase {
            query: "histogram_count(recorded_hist)",
            expected: 5.0,
        },
        QueryValueCase {
            query: "sum by(raw)(sum_over_time(recorded_float[1m]))",
            expected: 2.0,
        },
    ] {
        let samples = instant_vector(&engine, case.query).await;
        assert2::assert!(samples.len() == 4, "{}", case.query);
        assert2::assert!(
            raw_label_values(&samples) == values.iter().cloned().collect::<BTreeSet<_>>(),
            "{}",
            case.query
        );
        for sample in samples {
            assert2::assert!(
                sample.ts_ms == 30_000 && sample.value == SampleValue::Float(case.expected)
            );
        }
    }
    let QueryResult::InstantVector(samples) = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"recorded_float{raw!="�"}"#,
            30_000,
        )
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 3);
    assert2::assert!(
        samples
            .iter()
            .all(|sample| sample.labels.get_value("raw").unwrap().as_bytes() != "�".as_bytes())
    );
    for case in [
        QueryBytesCase {
            query: r#"recorded_float{raw="\xff"}"#,
            expected: vec![vec![0xff]],
        },
        QueryBytesCase {
            query: r#"recorded_float{raw!="\xff",raw!="�"}"#,
            expected: vec![vec![0xfe], b"__krabka_bytes_ff".to_vec()],
        },
        QueryBytesCase {
            query: r#"sum_over_time(recorded_float{raw="\xfe"}[1m])"#,
            expected: vec![vec![0xfe]],
        },
    ] {
        let samples = instant_vector(engine, case.query).await;
        assert2::assert!(
            samples
                .iter()
                .map(|sample| sample.labels.get_value("raw").unwrap().as_bytes().to_vec())
                .collect::<BTreeSet<_>>()
                == case.expected.into_iter().collect::<BTreeSet<_>>(),
            "{}",
            case.query
        );
        for sample in samples {
            assert2::assert!(sample.value == SampleValue::Float(2.0));
        }
    }
    assert2::assert!(
        engine
            .query_instant(
                &tenant_id("tenant-a"),
                r#"recorded_float{raw=~"\xff"}"#,
                30_000
            )
            .await
            .is_err()
    );
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-b"), "recorded_float", 30_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(samples.is_empty());
}
