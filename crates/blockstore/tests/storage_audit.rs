//! The offline storage audit, against one injected fault at a time.
//!
//! Every case starts from a healthy store of one signal that holds tenants
//! `t` and `u`, injects one fault into tenant `t`, and compares the whole
//! report with the diagnosis the fault must produce. The detail text is for
//! a person and not a contract, so the comparison blanks it after it checks
//! that each finding has one.

mod storage_fixtures;

use std::{collections::BTreeSet, sync::Arc, time::SystemTime};

use arrow::{
    array::{Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use assert2::{assert, check};
use futures::StreamExt as _;
use krabka_blockstore::{
    COL_FINGERPRINT, COL_TIMESTAMP, ErasureRequest, PERSISTED_BLOCK_FORMAT_KEY,
    STORAGE_AUDIT_SCHEMA_VERSION, StorageAuditError, StorageAuditOptions, StorageAuditReport,
    StorageFinding, StorageFindingKind, StorageSignal, audit_store,
};
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path};
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use storage_fixtures::{
    PROFILE_INDEX, TRACE_INDEX, block_key, healthy, later, listed_keys, publish, put_block,
    put_bytes, sidecar_key, store,
};

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    MissingBlock,
    CorruptBlock,
    UnsupportedBlock,
    OrphanBlock,
    YoungBlock,
    OrphanSidecar,
    MissingSidecar,
    SplitBrain,
    CorruptManifest,
    UnsupportedManifest,
    NoIndex,
    CorruptPayload,
    CorruptDeleteMarker,
    ForeignErasureRequest,
    StaleFrontier,
}

const TENANTS: [&str; 2] = ["t", "u"];

fn finding(
    kind: StorageFindingKind,
    signal: StorageSignal,
    tenant: Option<&str>,
    path: &str,
) -> StorageFinding {
    StorageFinding::new(kind, signal, tenant.map(str::to_string), path, "")
}

async fn keys_under(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let mut keys: Vec<String> = store
        .list(Some(&Path::from(prefix)))
        .map(|meta| meta.unwrap().location.to_string())
        .collect()
        .await;
    keys.sort();
    keys
}

async fn put_unsupported_block(store: &Arc<dyn ObjectStore>, key: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![7_u64])),
            Arc::new(Int64Array::from(vec![10_i64])),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            PERSISTED_BLOCK_FORMAT_KEY.to_string(),
            Some("99".to_string()),
        )]))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    store
        .put(&Path::from(key), PutPayload::from(bytes))
        .await
        .unwrap();
}

fn index_key(signal: StorageSignal) -> &'static str {
    match signal {
        StorageSignal::Traces => TRACE_INDEX,
        StorageSignal::Profiles => PROFILE_INDEX,
        StorageSignal::Metrics | StorageSignal::Logs => "",
    }
}

fn log_manifest(tenant: &str) -> String {
    format!("tenant={tenant}/index/logs/manifest.json")
}

async fn inject(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    fault: Fault,
) -> Vec<StorageFinding> {
    let live = healthy(store, signal, &TENANTS).await;
    let block = &live[0];
    let extra = block_key(signal, "t", 10, 19);
    let t = Some("t");
    match fault {
        Fault::None => Vec::new(),
        Fault::MissingBlock => {
            store.delete(&Path::from(block.as_str())).await.unwrap();
            let path = if signal == StorageSignal::Metrics {
                sidecar_key(signal, block)
            } else {
                block.clone()
            };
            vec![finding(
                StorageFindingKind::DanglingIndexEntry,
                signal,
                t,
                &path,
            )]
        }
        Fault::CorruptBlock => {
            put_bytes(store, block, b"not a parquet file").await;
            vec![finding(StorageFindingKind::CorruptBlock, signal, t, block)]
        }
        Fault::UnsupportedBlock => {
            put_unsupported_block(store, block).await;
            vec![finding(
                StorageFindingKind::UnsupportedFormat,
                signal,
                t,
                block,
            )]
        }
        Fault::OrphanBlock => {
            put_block(store, signal, &extra).await;
            vec![finding(StorageFindingKind::Orphan, signal, t, &extra)]
        }
        Fault::YoungBlock => {
            put_block(store, signal, &extra).await;
            vec![finding(StorageFindingKind::Pending, signal, t, &extra)]
        }
        Fault::OrphanSidecar => {
            let sidecar = sidecar_key(signal, &extra);
            put_bytes(store, &sidecar, b"symbols").await;
            vec![finding(
                StorageFindingKind::OrphanSidecar,
                signal,
                t,
                &sidecar,
            )]
        }
        Fault::MissingSidecar => {
            store
                .delete(&Path::from(sidecar_key(signal, block)))
                .await
                .unwrap();
            vec![finding(
                StorageFindingKind::MissingSidecar,
                signal,
                t,
                block,
            )]
        }
        Fault::SplitBrain => {
            let overlapping = block_key(signal, "t", 5, 14);
            put_block(store, signal, &overlapping).await;
            let both = [block.clone(), overlapping];
            let u_blocks = [live[1].clone()];
            publish(
                store,
                signal,
                &[("t", &both), ("u", &u_blocks)],
                &both.iter().chain(&u_blocks).cloned().collect(),
            )
            .await;
            vec![finding(StorageFindingKind::WalOverlap, signal, t, block)]
        }
        Fault::CorruptManifest
        | Fault::UnsupportedManifest
        | Fault::NoIndex
        | Fault::CorruptPayload => inject_index_fault(store, signal, fault).await,
        Fault::CorruptDeleteMarker => {
            let marker = "mimir-tenant-deletions/t.json";
            put_bytes(
                store,
                marker,
                br#"{"tenant_id":"t","objects":["metrics/u/float/x.parquet"]}"#,
            )
            .await;
            vec![finding(
                StorageFindingKind::UnreadableDeleteState,
                signal,
                t,
                marker,
            )]
        }
        Fault::ForeignErasureRequest => {
            let request = ErasureRequest::new("u", "{job=\"a\"}", Vec::new(), 0, 1, 2);
            let key = format!("metric-erasure-requests/t/{}.json", request.id);
            store
                .put(
                    &Path::from(key.as_str()),
                    PutPayload::from(serde_json::to_vec(&request).unwrap()),
                )
                .await
                .unwrap();
            vec![finding(
                StorageFindingKind::UnreadableDeleteState,
                signal,
                t,
                &key,
            )]
        }
        Fault::StaleFrontier => {
            let frontier = "index/logs/compaction-frontier.json";
            put_bytes(
                store,
                frontier,
                br#"{"version":1,"compacted_through_ns":0,"partition_offsets":{"0":50}}"#,
            )
            .await;
            vec![finding(
                StorageFindingKind::StaleFrontier,
                signal,
                None,
                frontier,
            )]
        }
    }
}

async fn inject_index_fault(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    fault: Fault,
) -> Vec<StorageFinding> {
    let t = Some("t");
    match fault {
        Fault::CorruptManifest => {
            if signal == StorageSignal::Logs {
                put_bytes(store, &log_manifest("t"), b"{").await;
                return vec![finding(
                    StorageFindingKind::UnreadableManifest,
                    signal,
                    t,
                    &log_manifest("t"),
                )];
            }
            let snapshots =
                keys_under(store, &index_key(signal).replace(".json", "/snapshots")).await;
            let latest = snapshots.last().unwrap();
            put_bytes(store, latest, b"{").await;
            vec![finding(
                StorageFindingKind::UnreadableManifest,
                signal,
                None,
                latest,
            )]
        }
        Fault::UnsupportedManifest => {
            if signal == StorageSignal::Logs {
                put_bytes(
                    store,
                    &log_manifest("t"),
                    br#"{"format_version":99,"series":[],"blocks":[]}"#,
                )
                .await;
                return vec![finding(
                    StorageFindingKind::UnsupportedFormat,
                    signal,
                    t,
                    &log_manifest("t"),
                )];
            }
            let snapshots =
                keys_under(store, &index_key(signal).replace(".json", "/snapshots")).await;
            let latest = snapshots.last().unwrap();
            put_bytes(store, latest, br#"{"v":99,"t":{}}"#).await;
            vec![finding(
                StorageFindingKind::UnsupportedFormat,
                signal,
                None,
                latest,
            )]
        }
        Fault::NoIndex => {
            if signal == StorageSignal::Logs {
                for tenant in TENANTS {
                    store
                        .delete(&Path::from(log_manifest(tenant)))
                        .await
                        .unwrap();
                }
                return TENANTS
                    .iter()
                    .map(|tenant| {
                        finding(
                            StorageFindingKind::UnreadableManifest,
                            signal,
                            Some(tenant),
                            &log_manifest(tenant),
                        )
                    })
                    .collect();
            }
            let prefix = index_key(signal).trim_end_matches(".json").to_string();
            for key in keys_under(store, &prefix).await {
                store.delete(&Path::from(key)).await.unwrap();
            }
            vec![finding(
                StorageFindingKind::UnreadableManifest,
                signal,
                None,
                index_key(signal),
            )]
        }
        Fault::CorruptPayload => {
            let payloads = keys_under(
                store,
                &index_key(signal).replace(".json", "/payloads/tenant=t"),
            )
            .await;
            let payload = payloads.first().unwrap();
            put_bytes(store, payload, b"tampered").await;
            vec![finding(
                StorageFindingKind::ChecksumMismatch,
                signal,
                t,
                payload,
            )]
        }
        _ => unreachable!("{fault:?} is not an index fault"),
    }
}

fn cases() -> Vec<(StorageSignal, Fault)> {
    let every_signal = [
        Fault::None,
        Fault::MissingBlock,
        Fault::CorruptBlock,
        Fault::UnsupportedBlock,
        Fault::OrphanBlock,
        Fault::YoungBlock,
        Fault::SplitBrain,
    ];
    let mut cases: Vec<_> = StorageSignal::ALL
        .into_iter()
        .flat_map(|signal| every_signal.map(|fault| (signal, fault)))
        .collect();
    cases.extend([
        (StorageSignal::Profiles, Fault::OrphanSidecar),
        (StorageSignal::Profiles, Fault::MissingSidecar),
        (StorageSignal::Metrics, Fault::CorruptDeleteMarker),
        (StorageSignal::Metrics, Fault::ForeignErasureRequest),
        (StorageSignal::Logs, Fault::StaleFrontier),
    ]);
    for signal in [
        StorageSignal::Logs,
        StorageSignal::Traces,
        StorageSignal::Profiles,
    ] {
        cases.extend([
            (signal, Fault::CorruptManifest),
            (signal, Fault::UnsupportedManifest),
            (signal, Fault::NoIndex),
        ]);
    }
    for signal in [StorageSignal::Traces, StorageSignal::Profiles] {
        cases.push((signal, Fault::CorruptPayload));
    }
    cases
}

#[tokio::test]
async fn each_injected_fault_produces_its_diagnosis() {
    for (signal, fault) in cases() {
        let store = store();
        let expected = inject(&store, signal, fault).await;
        let mut options = StorageAuditOptions::new(later());
        if matches!(fault, Fault::YoungBlock) {
            options.now = SystemTime::now();
        }
        let listed = listed_keys(&store).await.len();

        let mut report = audit_store(&store, &options).await.unwrap();

        for finding in &mut report.findings {
            check!(
                !finding.detail.is_empty(),
                "{signal} {fault:?}: {finding:?}"
            );
            finding.detail.clear();
        }
        let expected = StorageAuditReport::new(options.scope(), listed, 0, expected);
        check!(report == expected, "{signal} {fault:?}");
    }
}

#[tokio::test]
async fn an_audit_does_not_change_the_store() {
    let store = store();
    inject(&store, StorageSignal::Profiles, Fault::OrphanBlock).await;
    inject(&store, StorageSignal::Logs, Fault::StaleFrontier).await;
    let before = listed_keys(&store).await;

    let report = audit_store(&store, &StorageAuditOptions::new(later()))
        .await
        .unwrap();

    check!(report.read_only);
    check!(report.schema_version == STORAGE_AUDIT_SCHEMA_VERSION);
    check!(listed_keys(&store).await == before);
}

#[tokio::test]
async fn a_scoped_audit_reports_only_its_tenant_and_signal() {
    let store = store();
    inject(&store, StorageSignal::Traces, Fault::OrphanBlock).await;
    inject(&store, StorageSignal::Metrics, Fault::OrphanBlock).await;
    let orphan = block_key(StorageSignal::Traces, "t", 10, 19);

    let cases = [
        (Some(StorageSignal::Traces), Some("t"), vec![orphan.clone()]),
        (Some(StorageSignal::Traces), Some("u"), Vec::new()),
        (
            None,
            Some("t"),
            vec![block_key(StorageSignal::Metrics, "t", 10, 19), orphan],
        ),
        (Some(StorageSignal::Logs), None, Vec::new()),
    ];
    for (signal, tenant, expected) in cases {
        let mut options = StorageAuditOptions::new(later());
        options.signal = signal;
        options.tenant = tenant.map(str::to_string);

        let report = audit_store(&store, &options).await.unwrap();

        let paths: Vec<_> = report
            .findings
            .into_iter()
            .map(|finding| finding.path)
            .collect();
        check!(paths == expected, "{signal:?} {tenant:?}");
    }
}

#[tokio::test]
async fn a_healthy_store_of_every_signal_is_clean() {
    let store = store();
    let mut objects = BTreeSet::new();
    for signal in StorageSignal::ALL {
        objects.extend(healthy(&store, signal, &TENANTS).await);
    }

    let report = audit_store(&store, &StorageAuditOptions::new(later()))
        .await
        .unwrap();

    check!(report.findings.is_empty());
    check!(!report.has_damage());
    check!(report.objects_unclassified == 0);
    check!(objects.len() == 8);
}

#[tokio::test]
async fn keys_outside_every_grammar_are_counted_and_not_reported() {
    let store = store();
    for key in [
        "README",
        "rules/t/group.yaml",
        "metrics/t/unknown-kind/x.parquet",
    ] {
        put_bytes(&store, key, b"x").await;
    }

    let report = audit_store(&store, &StorageAuditOptions::new(later()))
        .await
        .unwrap();

    check!((report.objects_listed, report.objects_unclassified) == (3, 3));
    check!(report.findings.is_empty());
}

#[tokio::test]
async fn verify_data_is_recorded_and_reads_every_row() {
    let store = store();
    healthy(&store, StorageSignal::Metrics, &TENANTS).await;
    let mut options = StorageAuditOptions::new(later());
    options.verify_data = true;

    let report = audit_store(&store, &options).await.unwrap();

    check!(report.scope.verify_data);
    check!(report.findings.is_empty());
}

#[test]
fn a_report_round_trips_and_a_future_version_is_refused() {
    let report = StorageAuditReport::new(
        StorageAuditOptions::new(later()).scope(),
        2,
        0,
        vec![finding(
            StorageFindingKind::Orphan,
            StorageSignal::Traces,
            Some("t"),
            "traces/t/00000/a.parquet",
        )],
    );
    let bytes = serde_json::to_vec(&report).unwrap();
    check!(StorageAuditReport::from_json(&bytes).unwrap() == report);

    let mut future: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    future["schema_version"] = serde_json::json!(STORAGE_AUDIT_SCHEMA_VERSION + 1);
    let refused = StorageAuditReport::from_json(&serde_json::to_vec(&future).unwrap());
    assert!(let Err(StorageAuditError::UnsupportedSchemaVersion { .. }) = refused);
}

#[test]
fn finding_kind_names_are_stable() {
    let names: Vec<_> = StorageFindingKind::ALL
        .iter()
        .map(|kind| serde_json::to_value(kind).unwrap())
        .collect();
    check!(
        names
            == [
                "checksum_mismatch",
                "corrupt_block",
                "dangling_index_entry",
                "missing_sidecar",
                "orphan",
                "orphan_sidecar",
                "pending",
                "stale_frontier",
                "unreadable_delete_state",
                "unreadable_manifest",
                "unsupported_format",
                "wal_overlap",
            ]
            .map(serde_json::Value::from)
    );
    for kind in StorageFindingKind::ALL {
        check!(kind.as_str().parse::<StorageFindingKind>().unwrap() == kind);
    }
}
