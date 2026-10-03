//! Deployment backup sets: one sealed cut over every store of a deployment
//! and the broker snapshot that goes with it.
//!
//! The broker side is a scripted fake behind [`BrokerState`]. The suite in
//! `crates/integration/tests/backup_restore.rs` drives the same functions
//! against a real broker and all four signals.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use async_trait::async_trait;
use krabka_blockstore::{
    AuditFinding, AuditReport, BACKUP_MANIFEST_PATH, BackupManifest, BackupObject, BrokerFinding,
    BrokerSnapshot, BrokerState, CUT_MANIFEST_PATH, CutPart, DeploymentBackupPlan, DeploymentCut,
    DeploymentPart, DrainedGroup, GroupOffset, RecoveryError, RestoreReport, TenantId, WalOffset,
    audit_deployment_backup, backup_deployment, load_backup_manifest, restore_backup,
    restore_deployment_backup,
};
use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path as ObjectPath};
use sha2::{Digest as _, Sha256};

const WAL_TOPIC: &str = "__krabka_metrics_wal";
const BLOCK_BUILDER: &str = "krabka-metrics-block-builder";

/// Hands out one scripted snapshot per capture, and repeats the last one.
struct ScriptedBroker(Mutex<VecDeque<BrokerSnapshot>>);

impl ScriptedBroker {
    fn new(snapshots: impl IntoIterator<Item = BrokerSnapshot>) -> Self {
        Self(Mutex::new(snapshots.into_iter().collect()))
    }
}

#[async_trait]
impl BrokerState for ScriptedBroker {
    async fn capture(&self) -> Result<BrokerSnapshot, String> {
        let mut snapshots = self.0.lock().expect("scripted broker");
        if snapshots.len() > 1 {
            Ok(snapshots.pop_front().expect("one snapshot"))
        } else {
            snapshots
                .front()
                .cloned()
                .ok_or_else(|| "no snapshot".into())
        }
    }
}

fn snapshot(wal_next: i64, committed: i64) -> BrokerSnapshot {
    BrokerSnapshot {
        wal_offsets: vec![WalOffset {
            topic: WAL_TOPIC.into(),
            partition: 0,
            next_offset: wal_next,
        }],
        group_offsets: vec![GroupOffset {
            group: BLOCK_BUILDER.into(),
            topic: WAL_TOPIC.into(),
            partition: 0,
            next_offset: committed,
        }],
    }
}

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

async fn put(store: &Arc<dyn ObjectStore>, path: &str, bytes: &[u8]) {
    store
        .put(&ObjectPath::from(path), bytes.to_vec().into())
        .await
        .expect("put");
}

async fn paths(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut paths = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Two parts of a live deployment: a signal bucket and a local state
/// directory.
async fn live_parts() -> Vec<DeploymentPart> {
    let metrics = store();
    put(
        &metrics,
        "metrics/tenant-a/float/partition=0000000000/a.parquet",
        b"block",
    )
    .await;
    put(
        &metrics,
        "metrics/tenant-a/float/partition=0000000000/a.index",
        b"index",
    )
    .await;
    put(&metrics, "mimir-configs/ruler/tenant-a.yaml", b"rules").await;
    let logs_state = store();
    put(&logs_state, "log-delete-requests.json", b"[]").await;
    vec![
        DeploymentPart {
            name: "metrics".into(),
            store: metrics,
        },
        DeploymentPart {
            name: "logs-querier-state".into(),
            store: logs_state,
        },
    ]
}

fn plan(parts: Vec<DeploymentPart>) -> DeploymentBackupPlan {
    DeploymentBackupPlan {
        cut_id: "cut-1".into(),
        broker_capture: "0001762000000000".into(),
        drained_groups: vec![DrainedGroup {
            group: BLOCK_BUILDER.into(),
            topic: WAL_TOPIC.into(),
        }],
        parts,
        omitted_parts: Vec::new(),
    }
}

fn empty_targets(names: &[&str]) -> Vec<DeploymentPart> {
    names
        .iter()
        .map(|name| DeploymentPart {
            name: (*name).into(),
            store: store(),
        })
        .collect()
}

#[tokio::test]
async fn a_sealed_cut_names_every_part_by_digest_and_restores_every_part() {
    let backup = store();
    let report = backup_deployment(
        &ScriptedBroker::new([snapshot(3, 3)]),
        &backup,
        &plan(live_parts().await),
    )
    .await
    .expect("backup");

    let stored = backup
        .get(&ObjectPath::from(CUT_MANIFEST_PATH))
        .await
        .expect("cut")
        .bytes()
        .await
        .expect("cut bytes");
    check!(report.cut_sha256 == sha256(&stored));
    let metrics_manifest = backup
        .get(&ObjectPath::from(format!(
            "parts/metrics/{BACKUP_MANIFEST_PATH}"
        )))
        .await
        .expect("metrics manifest")
        .bytes()
        .await
        .expect("metrics manifest bytes");
    let logs_manifest = backup
        .get(&ObjectPath::from(format!(
            "parts/logs-querier-state/{BACKUP_MANIFEST_PATH}"
        )))
        .await
        .expect("logs manifest")
        .bytes()
        .await
        .expect("logs manifest bytes");
    check!(
        report.cut
            == DeploymentCut {
                schema_version: 1,
                cut_id: "cut-1".into(),
                broker_capture: "0001762000000000".into(),
                broker: snapshot(3, 3),
                parts: vec![
                    CutPart {
                        name: "logs-querier-state".into(),
                        manifest_sha256: sha256(&logs_manifest),
                        object_count: 1,
                        byte_count: 2,
                    },
                    CutPart {
                        name: "metrics".into(),
                        manifest_sha256: sha256(&metrics_manifest),
                        object_count: 3,
                        byte_count: 15,
                    },
                ],
                omitted_parts: vec![],
            }
    );

    let audit = audit_deployment_backup(&backup).await.expect("audit");
    check!(audit.cut == report.cut);
    assert!(audit.parts.values().all(AuditReport::is_clean));

    let targets = empty_targets(&["metrics", "logs-querier-state"]);
    let restored =
        restore_deployment_backup(&backup, &ScriptedBroker::new([snapshot(3, 3)]), &targets)
            .await
            .expect("restore");
    check!(restored.broker == snapshot(3, 3));
    check!(
        restored
            .parts
            .iter()
            .map(|(name, part)| (name.as_str(), part.created, part.already_present))
            .collect::<Vec<_>>()
            == [("logs-querier-state", 1, 0), ("metrics", 3, 0)]
    );
    for (live, target) in live_parts().await.iter().zip(&targets) {
        check!(paths(&target.store).await == paths(&live.store).await);
    }

    // A repeated restore resumes and copies nothing.
    let repeated =
        restore_deployment_backup(&backup, &ScriptedBroker::new([snapshot(3, 3)]), &targets)
            .await
            .expect("repeated restore");
    check!(
        repeated
            .parts
            .values()
            .map(|part| (part.created, part.already_present))
            .collect::<Vec<_>>()
            == [(0, 1), (0, 3)]
    );
}

#[tokio::test]
async fn a_backup_refuses_a_lagging_block_builder_and_a_moving_broker() {
    struct Case {
        name: &'static str,
        broker: Vec<BrokerSnapshot>,
        findings: Vec<BrokerFinding>,
    }
    let cases = [
        Case {
            name: "block builder lags the WAL",
            broker: vec![snapshot(3, 2)],
            findings: vec![BrokerFinding::UndrainedGroup {
                group: BLOCK_BUILDER.into(),
                topic: WAL_TOPIC.into(),
                partition: 0,
                committed: Some(2),
                wal_next_offset: 3,
            }],
        },
        Case {
            name: "the block builder has no committed offset",
            broker: vec![BrokerSnapshot {
                group_offsets: vec![],
                ..snapshot(3, 3)
            }],
            findings: vec![BrokerFinding::UndrainedGroup {
                group: BLOCK_BUILDER.into(),
                topic: WAL_TOPIC.into(),
                partition: 0,
                committed: None,
                wal_next_offset: 3,
            }],
        },
        Case {
            name: "a writer moved during the copy",
            broker: vec![snapshot(3, 3), snapshot(4, 3)],
            findings: vec![BrokerFinding::WalOffset {
                topic: WAL_TOPIC.into(),
                partition: 0,
                expected: Some(3),
                actual: Some(4),
            }],
        },
    ];
    for case in cases {
        let backup = store();
        let error = backup_deployment(
            &ScriptedBroker::new(case.broker),
            &backup,
            &plan(live_parts().await),
        )
        .await
        .expect_err(case.name);
        let RecoveryError::BrokerMismatch { findings, .. } = error else {
            panic!("{}: {error}", case.name);
        };
        check!(findings == case.findings, "{}", case.name);
        // No cut is sealed, so the set is not restorable.
        check!(
            !paths(&backup)
                .await
                .contains(&CUT_MANIFEST_PATH.to_string()),
            "{}",
            case.name
        );
    }
}

#[tokio::test]
async fn a_restore_refuses_a_partial_or_mixed_set_before_it_writes() {
    async fn sealed() -> Arc<dyn ObjectStore> {
        let backup = store();
        backup_deployment(
            &ScriptedBroker::new([snapshot(3, 3)]),
            &backup,
            &plan(live_parts().await),
        )
        .await
        .expect("backup");
        backup
    }
    async fn other_cut_part() -> Vec<(String, Vec<u8>)> {
        let other = store();
        let mut other_plan = plan(live_parts().await);
        other_plan.cut_id = "cut-2".into();
        backup_deployment(&ScriptedBroker::new([snapshot(3, 3)]), &other, &other_plan)
            .await
            .expect("other backup");
        let mut objects = Vec::new();
        for path in paths(&other).await {
            if path.starts_with("parts/metrics/") {
                let bytes = other
                    .get(&ObjectPath::from(path.as_str()))
                    .await
                    .expect("get")
                    .bytes()
                    .await
                    .expect("bytes");
                objects.push((path, bytes.to_vec()));
            }
        }
        objects
    }

    #[derive(Clone, Copy, Debug)]
    enum Damage {
        Unsealed,
        MissingPart,
        PartOfAnotherCut,
        UnlistedObject,
        CorruptObject,
        MissingTarget,
        ExtraTarget,
        OtherBrokerSnapshot,
        NonEmptyTarget,
    }
    for damage in [
        Damage::Unsealed,
        Damage::MissingPart,
        Damage::PartOfAnotherCut,
        Damage::UnlistedObject,
        Damage::CorruptObject,
        Damage::MissingTarget,
        Damage::ExtraTarget,
        Damage::OtherBrokerSnapshot,
        Damage::NonEmptyTarget,
    ] {
        let backup = sealed().await;
        let mut target_names = vec!["metrics", "logs-querier-state"];
        let mut broker = snapshot(3, 3);
        match damage {
            Damage::Unsealed => backup
                .delete(&ObjectPath::from(CUT_MANIFEST_PATH))
                .await
                .expect("delete"),
            Damage::MissingPart => backup
                .delete(&ObjectPath::from(format!(
                    "parts/logs-querier-state/{BACKUP_MANIFEST_PATH}"
                )))
                .await
                .expect("delete"),
            Damage::PartOfAnotherCut => {
                for (path, bytes) in other_cut_part().await {
                    put(&backup, &path, &bytes).await;
                }
            }
            Damage::UnlistedObject => put(&backup, "parts/profiles/blocks/x.parquet", b"x").await,
            Damage::CorruptObject => {
                put(
                    &backup,
                    "parts/metrics/mimir-configs/ruler/tenant-a.yaml",
                    b"other",
                )
                .await;
            }
            Damage::MissingTarget => target_names.truncate(1),
            Damage::ExtraTarget => target_names.push("profiles"),
            Damage::OtherBrokerSnapshot => broker = snapshot(5, 5),
            Damage::NonEmptyTarget => {}
        }
        let targets = empty_targets(&target_names);
        if matches!(damage, Damage::NonEmptyTarget) {
            put(&targets[1].store, "unexpected.json", b"live").await;
        }

        let error = restore_deployment_backup(&backup, &ScriptedBroker::new([broker]), &targets)
            .await
            .expect_err("a damaged set is refused");
        let refused_as = match error {
            RecoveryError::MixedSet(_) => "mixed",
            RecoveryError::UnsafeTarget { .. } => "unsafe",
            RecoveryError::BrokerMismatch { .. } => "broker",
            other => panic!("{damage:?}: {other}"),
        };
        let expected = match damage {
            Damage::Unsealed
            | Damage::MissingPart
            | Damage::PartOfAnotherCut
            | Damage::UnlistedObject
            | Damage::MissingTarget
            | Damage::ExtraTarget => "mixed",
            Damage::CorruptObject | Damage::NonEmptyTarget => "unsafe",
            Damage::OtherBrokerSnapshot => "broker",
        };
        check!(refused_as == expected, "{damage:?}");
        // The restore wrote nothing into any target.
        for target in &targets {
            let found = paths(&target.store).await;
            check!(
                found.is_empty() || found == ["unexpected.json"],
                "{damage:?}: {found:?}"
            );
        }
    }
}

/// A broker whose partition ends in transaction markers after `records`.
struct MarkerTail {
    snapshot: BrokerSnapshot,
    records: u64,
}

#[async_trait]
impl BrokerState for MarkerTail {
    async fn capture(&self) -> Result<BrokerSnapshot, String> {
        Ok(self.snapshot.clone())
    }

    async fn records_between(
        &self,
        _topic: &str,
        _partition: i32,
        _offset: i64,
        _next_offset: i64,
    ) -> Result<u64, String> {
        Ok(self.records)
    }
}

#[tokio::test]
async fn a_block_builder_behind_only_transaction_markers_is_drained() {
    for (records, sealed) in [(0, true), (1, false)] {
        let backup = store();
        let result = backup_deployment(
            &MarkerTail {
                snapshot: snapshot(4, 3),
                records,
            },
            &backup,
            &plan(live_parts().await),
        )
        .await;
        check!(result.is_ok() == sealed, "{records} pending records");
    }
}

#[tokio::test]
async fn a_block_builder_with_no_committed_offset_on_an_empty_wal_is_drained() {
    // A group with no committed offset reads from the start of the WAL, so it
    // is drained only when the WAL has no record.
    let broker = BrokerSnapshot {
        group_offsets: vec![],
        ..snapshot(0, 0)
    };
    let backup = store();
    let report = backup_deployment(
        &ScriptedBroker::new([broker.clone()]),
        &backup,
        &plan(live_parts().await),
    )
    .await
    .expect("an empty WAL needs no commit");
    check!(report.cut.broker == broker);
}

#[tokio::test]
async fn a_cut_records_the_parts_that_the_operator_omitted() {
    let backup = store();
    let mut omitting = plan(live_parts().await);
    omitting.omitted_parts = vec!["traces".into(), "profiles".into()];
    let report = backup_deployment(&ScriptedBroker::new([snapshot(3, 3)]), &backup, &omitting)
        .await
        .expect("backup");
    check!(report.cut.omitted_parts == ["profiles", "traces"]);
    let audit = audit_deployment_backup(&backup).await.expect("audit");
    check!(audit.cut.omitted_parts == ["profiles", "traces"]);
    let restored = restore_deployment_backup(
        &backup,
        &ScriptedBroker::new([snapshot(3, 3)]),
        &empty_targets(&["metrics", "logs-querier-state"]),
    )
    .await
    .expect("restore");
    check!(restored.cut.omitted_parts == ["profiles", "traces"]);

    // A part name is a part or an omitted part, not both.
    for omitted in ["metrics", "Metrics"] {
        let mut refused = plan(live_parts().await);
        refused.omitted_parts = vec![omitted.into()];
        let error = backup_deployment(&ScriptedBroker::new([snapshot(3, 3)]), &store(), &refused)
            .await
            .expect_err(omitted);
        check!(
            matches!(error, RecoveryError::InvalidManifest(_)),
            "{omitted}"
        );
    }
}

#[tokio::test]
async fn a_cut_written_without_omitted_parts_still_audits() {
    let backup = store();
    backup_deployment(
        &ScriptedBroker::new([snapshot(3, 3)]),
        &backup,
        &plan(live_parts().await),
    )
    .await
    .expect("backup");
    let path = ObjectPath::from(CUT_MANIFEST_PATH);
    let mut cut: serde_json::Value = serde_json::from_slice(
        &backup
            .get(&path)
            .await
            .expect("cut")
            .bytes()
            .await
            .expect("cut bytes"),
    )
    .expect("cut JSON");
    check!(cut["omitted_parts"] == serde_json::json!([]));
    cut.as_object_mut()
        .expect("cut object")
        .remove("omitted_parts");
    put(
        &backup,
        CUT_MANIFEST_PATH,
        &serde_json::to_vec_pretty(&cut).expect("JSON"),
    )
    .await;

    let audit = audit_deployment_backup(&backup).await.expect("audit");
    check!(audit.cut.omitted_parts == Vec::<String>::new());
    assert!(audit.parts.values().all(AuditReport::is_clean));
}

#[tokio::test]
async fn a_second_backup_into_a_sealed_root_is_refused() {
    let backup = store();
    let broker = ScriptedBroker::new([snapshot(3, 3)]);
    backup_deployment(&broker, &backup, &plan(live_parts().await))
        .await
        .expect("first backup");

    let error = backup_deployment(&broker, &backup, &plan(live_parts().await))
        .await
        .expect_err("a sealed root is refused");
    assert!(matches!(error, RecoveryError::MixedSet(_)));
}

/// A manifest written by the v0.4 release, byte for byte as that writer
/// encoded it.
fn version_1_manifest(objects: &[BackupObject]) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "tenant": "tenant-a",
        "cut_id": "cut-2027-02-01",
        "wal_offsets": [{"topic": WAL_TOPIC, "partition": 0, "next_offset": 42}],
        "objects": objects,
    })
}

#[tokio::test]
async fn a_version_1_backup_still_restores_and_a_future_version_is_refused_before_a_write() {
    let objects = vec![BackupObject {
        path: "blocks/a.parquet".into(),
        size: 5,
        sha256: sha256(b"block"),
    }];
    let expected = BackupManifest {
        schema_version: 1,
        tenant: Some(TenantId::new("tenant-a").expect("tenant")),
        part: None,
        cut_id: "cut-2027-02-01".into(),
        wal_offsets: vec![WalOffset {
            topic: WAL_TOPIC.into(),
            partition: 0,
            next_offset: 42,
        }],
        objects: objects.clone(),
    };

    for (version, accepted) in [(1, true), (3, false)] {
        let backup = store();
        put(&backup, "blocks/a.parquet", b"block").await;
        let mut manifest = version_1_manifest(&objects);
        manifest["schema_version"] = version.into();
        put(
            &backup,
            BACKUP_MANIFEST_PATH,
            &serde_json::to_vec_pretty(&manifest).expect("json"),
        )
        .await;
        let target = store();

        let restored = restore_backup(Arc::clone(&backup), Arc::clone(&target)).await;
        if accepted {
            check!(load_backup_manifest(backup.as_ref()).await.ok() == Some(expected.clone()));
            let report = restored.expect("a version 1 set restores");
            check!(
                report
                    == RestoreReport {
                        created: 1,
                        already_present: 0,
                        audit: AuditReport {
                            schema_version: 1,
                            tenant: Some(TenantId::new("tenant-a").expect("tenant")),
                            part: None,
                            cut_id: "cut-2027-02-01".into(),
                            manifest_sha256: report.audit.manifest_sha256.clone(),
                            read_only: true,
                            findings: vec![],
                        },
                    }
            );
            // The digest is of the manifest as the v0.4 writer encoded it.
            check!(
                report.audit.manifest_sha256
                    == sha256(&serde_json::to_vec_pretty(&expected).expect("json"))
            );
        } else {
            assert!(matches!(restored, Err(RecoveryError::InvalidManifest(_))));
            check!(paths(&target).await.is_empty(), "version {version}");
        }
    }
}

#[tokio::test]
async fn an_audit_of_a_damaged_part_names_the_damage() {
    let backup = store();
    backup_deployment(
        &ScriptedBroker::new([snapshot(3, 3)]),
        &backup,
        &plan(live_parts().await),
    )
    .await
    .expect("backup");
    backup
        .delete(&ObjectPath::from(
            "parts/metrics/metrics/tenant-a/float/partition=0000000000/a.index",
        ))
        .await
        .expect("delete");

    let audit = audit_deployment_backup(&backup).await.expect("audit");
    check!(audit.parts["logs-querier-state"].findings == []);
    check!(
        audit.parts["metrics"].findings
            == [AuditFinding::Missing {
                path: "metrics/tenant-a/float/partition=0000000000/a.index".into(),
            }]
    );
}
