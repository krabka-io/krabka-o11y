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

#[tokio::test]
async fn byte_recording_rules_survive_wal_compaction_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut seed = InMemoryMetricStore::new();
    seed.push_float(
        "tenant-a",
        labels(&[("__name__", "input_float")]),
        30_000,
        2.0,
    );
    seed.push_histogram(
        "tenant-a",
        labels(&[("__name__", "input_hist")]),
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
    for (input, output) in [
        ("input_float", "recorded_float"),
        ("input_hist", "recorded_hist"),
    ] {
        let query = values
            .iter()
            .map(|bytes| {
                let literal = PromqlString::from(bytes.clone()).quoted();
                format!("label_replace({input},\"raw\",{literal},\"\",\".*\")")
            })
            .collect::<Vec<_>>()
            .join(" or ");
        let materialized = evaluate_recording_rule(
            &seed_engine,
            &tenant_id("tenant-a"),
            output,
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
            output,
            &query,
            &BTreeMap::from([("raw".to_owned(), "collapsed".to_owned())]),
            30_000,
        )
        .await
        .unwrap_err();
        assert2::assert!(error.to_string().contains("same labelset"));
        records.extend(materialized);
    }
    for (index, record) in records.iter().enumerate() {
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
    let manifests = list_compaction_manifests(&object_store).await.unwrap();
    let cold = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(object_store.clone(), base.clone()),
        Some(BlockStore::new(object_store.clone(), base.clone())),
        &manifests,
    );
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
    for _ in 0..2 {
        compact_metric_blocks_once(
            &object_store,
            &BlockWriter::new(object_store.clone()),
            &ObjectStoreCompactionIndexSink::new(object_store.clone()),
            policy,
            DEFAULT_BLOCK_READ_MAX,
            &mut deferred,
        )
        .await
        .unwrap();
    }
    drop(object_store);
    // Reopening the filesystem and rebuilding solely from persisted sidecars
    // prevents an in-process label cache from hiding a lossy storage adapter.
    let reopened: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&objects).unwrap());
    let manifests = list_compaction_manifests(&reopened).await.unwrap();
    let cold = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(reopened.clone(), base.clone()),
        Some(BlockStore::new(reopened.clone(), base.clone())),
        &manifests,
    );
    let engine = PromqlEngine::new(Arc::new(cold), EngineOpts::default());
    check_byte_ledger(&engine, &values).await;
    drop(engine);
    let raw_matcher = |byte| krabka_blockstore::ByteLabelMatcher {
        name: "raw".to_owned(),
        op: krabka_blockstore::MatchOp::Eq,
        value: vec![byte],
    };
    // Both a different tenant and a disjoint closed time window must leave FE
    // intact. The matching request removes only FF across both sample kinds.
    for (tenant, start, end, byte) in [
        ("tenant-b", 30_000_000_000, 30_000_000_000, 0xfe),
        ("tenant-a", 31_000_000_000, 31_000_000_000, 0xfe),
        ("tenant-a", 30_000_000_000, 30_000_000_000, 0xff),
    ] {
        let request = krabka_blockstore::ErasureRequest::new(
            tenant,
            "byte ledger",
            Vec::new(),
            start,
            end,
            40_000_000_000,
        )
        .with_byte_matchers(vec![vec![raw_matcher(byte)]]);
        krabka_blockstore::put_erasure_request(
            &reopened,
            krabka_blockstore::ERASURE_REQUEST_PREFIX,
            &request,
        )
        .await
        .unwrap();
    }
    for _ in 0..2 {
        compact_metric_blocks_once(
            &reopened,
            &BlockWriter::new(reopened.clone()),
            &ObjectStoreCompactionIndexSink::new(reopened.clone()),
            policy,
            DEFAULT_BLOCK_READ_MAX,
            &mut deferred,
        )
        .await
        .unwrap();
    }
    drop(reopened);
    let reopened: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&objects).unwrap());
    let manifests = list_compaction_manifests(&reopened).await.unwrap();
    let cold = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(reopened.clone(), base.clone()),
        Some(BlockStore::new(reopened, base)),
        &manifests,
    );
    let engine = PromqlEngine::new(Arc::new(cold), EngineOpts::default());
    for (query, expected_value) in [
        ("recorded_float", 2.0),
        ("histogram_count(recorded_hist)", 5.0),
    ] {
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), query, 30_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 3);
        assert2::assert!(
            samples
                .iter()
                .map(|sample| sample.labels.get_value("raw").unwrap().as_bytes().to_vec())
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([
                    vec![0xfe],
                    "�".as_bytes().to_vec(),
                    b"__krabka_bytes_ff".to_vec()
                ])
        );
        for sample in samples {
            assert2::assert!(sample.value == SampleValue::Float(expected_value));
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

async fn check_byte_ledger<S: MetricStore>(engine: &PromqlEngine<S>, values: &[Vec<u8>; 4]) {
    for (query, value) in [
        ("sum by(raw)(recorded_float)", 2.0),
        ("histogram_count(recorded_hist)", 5.0),
        ("sum by(raw)(sum_over_time(recorded_float[1m]))", 2.0),
    ] {
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), query, 30_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 4, "{query}");
        assert2::assert!(
            samples
                .iter()
                .map(|sample| sample.labels.get_value("raw").unwrap().as_bytes().to_vec())
                .collect::<BTreeSet<_>>()
                == values.iter().cloned().collect::<BTreeSet<_>>(),
            "{query}"
        );
        for sample in samples {
            assert2::assert!(sample.ts_ms == 30_000 && sample.value == SampleValue::Float(value));
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
    for (query, expected) in [
        (r#"recorded_float{raw="\xff"}"#, vec![vec![0xff]]),
        (
            r#"recorded_float{raw!="\xff",raw!="�"}"#,
            vec![vec![0xfe], b"__krabka_bytes_ff".to_vec()],
        ),
        (
            r#"sum_over_time(recorded_float{raw="\xfe"}[1m])"#,
            vec![vec![0xfe]],
        ),
    ] {
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), query, 30_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert2::assert!(
            samples
                .iter()
                .map(|sample| sample.labels.get_value("raw").unwrap().as_bytes().to_vec())
                .collect::<BTreeSet<_>>()
                == expected.into_iter().collect::<BTreeSet<_>>(),
            "{query}"
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
