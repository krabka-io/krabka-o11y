//! Checksummed, tenant-scoped object-store backup and recovery.

use std::{collections::BTreeMap, sync::Arc};

use futures::TryStreamExt as _;
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::TenantId;

/// Completion marker written last in every backup set.
pub const BACKUP_MANIFEST_PATH: &str = ".krabka-recovery/manifest.json";
const BACKUP_SCHEMA_VERSION: u32 = 2;

/// The next offset to consume for one WAL partition at the backup cut.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WalOffset {
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

/// Immutable broker snapshot that contains the WAL offsets in this cut.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrokerSnapshot {
    pub id: String,
    pub sha256: String,
}

/// One immutable object in a backup set.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BackupObject {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

/// Identity and consistent-cut metadata for a tenant backup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupManifest {
    pub schema_version: u32,
    pub tenant: TenantId,
    pub cut_id: String,
    pub broker_snapshot: BrokerSnapshot,
    pub wal_offsets: Vec<WalOffset>,
    pub objects: Vec<BackupObject>,
}

/// A stable diagnosis against a [`BackupManifest`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditFinding {
    Missing {
        path: String,
    },
    Corrupt {
        path: String,
        expected_size: u64,
        actual_size: u64,
        expected_sha256: String,
        actual_sha256: String,
    },
    Orphan {
        path: String,
        actual_size: u64,
        actual_sha256: String,
    },
    Stale {
        path: String,
        expected_cut_id: String,
        actual_cut_id: String,
    },
    SplitBrain {
        path: String,
        expected_tenant: String,
        actual_tenant: String,
        expected_cut_id: String,
        actual_cut_id: String,
    },
}

/// Read-only comparison of one object-store prefix with its backup manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuditReport {
    pub schema_version: u32,
    pub tenant: TenantId,
    pub cut_id: String,
    pub manifest_sha256: String,
    pub read_only: bool,
    pub findings: Vec<AuditFinding>,
}

impl AuditReport {
    /// Whether the audited prefix exactly matches the manifest.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }
}

/// Result of an idempotent restore pass.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreReport {
    pub created: usize,
    pub already_present: usize,
    pub audit: AuditReport,
}

/// Exact offline scope required before repairing a recovery target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairScope {
    pub tenant: TenantId,
    pub cut_id: String,
    pub manifest_sha256: String,
    pub offline: bool,
    pub replace_corrupt: bool,
    pub delete_orphans: bool,
}

/// One stable, audit-logged repair mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairAction {
    pub operation: String,
    pub path: String,
}

/// Result of an explicitly scoped repair pass.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairReport {
    pub scope: RepairScope,
    pub before: AuditReport,
    pub actions: Vec<RepairAction>,
    pub after: AuditReport,
}

/// Backup, audit, or restore failure.
#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("object-store operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("backup manifest is invalid: {0}")]
    InvalidManifest(String),
    #[error("JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{operation} refused because the target is mixed or corrupt")]
    UnsafeTarget {
        operation: &'static str,
        report: Box<AuditReport>,
    },
}

/// Create or resume a checksummed backup under an empty, dedicated prefix.
///
/// The manifest is written last. A retry accepts objects that already match
/// and refuses any conflicting or unlisted object.
///
/// # Errors
///
/// Returns an error when the cut metadata is invalid, either store cannot be
/// read or written, or the backup prefix contains conflicting content.
pub async fn create_backup(
    source: Arc<dyn ObjectStore>,
    backup: Arc<dyn ObjectStore>,
    tenant: TenantId,
    cut_id: String,
    broker_snapshot: BrokerSnapshot,
    mut wal_offsets: Vec<WalOffset>,
) -> Result<RestoreReport, RecoveryError> {
    wal_offsets.sort();
    let manifest = BackupManifest {
        schema_version: BACKUP_SCHEMA_VERSION,
        tenant,
        cut_id,
        broker_snapshot,
        wal_offsets,
        objects: inventory(source.as_ref(), true).await?,
    };
    validate_manifest(&manifest)?;

    if let Some(existing) = load_backup_manifest_optional(backup.as_ref()).await?
        && existing != manifest
    {
        return Err(RecoveryError::InvalidManifest(
            "the backup prefix already contains a different completed cut".into(),
        ));
    }

    let before = audit_backup(backup.as_ref(), &manifest).await?;
    refuse_unsafe("backup", &before, true)?;
    let (created, already_present) =
        copy_manifest_objects(source.as_ref(), backup.as_ref(), &manifest).await?;
    let manifest_bytes = manifest_bytes(&manifest)?;
    let _ = put_create_or_equal(
        backup.as_ref(),
        &ObjectPath::from(BACKUP_MANIFEST_PATH),
        &manifest_bytes,
    )
    .await?;
    let audit = audit_backup(backup.as_ref(), &manifest).await?;
    refuse_unsafe("backup", &audit, false)?;
    Ok(RestoreReport {
        created,
        already_present,
        audit,
    })
}

/// Load and validate the completion manifest from a backup prefix.
///
/// # Errors
///
/// Returns an error when the manifest is absent, malformed, unsupported, or
/// cannot be read.
pub async fn load_backup_manifest(
    backup: &dyn ObjectStore,
) -> Result<BackupManifest, RecoveryError> {
    load_backup_manifest_optional(backup)
        .await?
        .ok_or_else(|| RecoveryError::InvalidManifest("backup has no completion manifest".into()))
}

/// Compare a prefix with a manifest without changing either.
///
/// # Errors
///
/// Returns an error when the manifest is invalid or an object cannot be read.
pub async fn audit_backup(
    store: &dyn ObjectStore,
    manifest: &BackupManifest,
) -> Result<AuditReport, RecoveryError> {
    audit(store, manifest, true).await
}

/// Compare a restored data prefix with a manifest without ignoring recovery metadata.
///
/// # Errors
///
/// Returns an error when the manifest is invalid or an object cannot be read.
pub async fn audit_recovery_target(
    store: &dyn ObjectStore,
    manifest: &BackupManifest,
) -> Result<AuditReport, RecoveryError> {
    audit(store, manifest, false).await
}

async fn audit(
    store: &dyn ObjectStore,
    manifest: &BackupManifest,
    ignore_completion_marker: bool,
) -> Result<AuditReport, RecoveryError> {
    validate_manifest(manifest)?;
    let actual = inventory(store, true).await?;
    let expected = manifest
        .objects
        .iter()
        .map(|object| (object.path.as_str(), object))
        .collect::<BTreeMap<_, _>>();
    let actual = actual
        .iter()
        .map(|object| (object.path.as_str(), object))
        .collect::<BTreeMap<_, _>>();
    let mut findings = Vec::new();

    if !ignore_completion_marker {
        match store.get(&ObjectPath::from(BACKUP_MANIFEST_PATH)).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                match serde_json::from_slice::<BackupManifest>(&bytes) {
                    Ok(found) if found == *manifest => {}
                    Ok(found)
                        if found.tenant == manifest.tenant && found.cut_id != manifest.cut_id =>
                    {
                        findings.push(AuditFinding::Stale {
                            path: BACKUP_MANIFEST_PATH.into(),
                            expected_cut_id: manifest.cut_id.clone(),
                            actual_cut_id: found.cut_id,
                        });
                    }
                    Ok(found) => findings.push(AuditFinding::SplitBrain {
                        path: BACKUP_MANIFEST_PATH.into(),
                        expected_tenant: manifest.tenant.to_string(),
                        actual_tenant: found.tenant.to_string(),
                        expected_cut_id: manifest.cut_id.clone(),
                        actual_cut_id: found.cut_id,
                    }),
                    Err(_) => findings.push(AuditFinding::SplitBrain {
                        path: BACKUP_MANIFEST_PATH.into(),
                        expected_tenant: manifest.tenant.to_string(),
                        actual_tenant: "<invalid>".into(),
                        expected_cut_id: manifest.cut_id.clone(),
                        actual_cut_id: "<invalid>".into(),
                    }),
                }
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }

    for object in &manifest.objects {
        match actual.get(object.path.as_str()) {
            None => findings.push(AuditFinding::Missing {
                path: object.path.clone(),
            }),
            Some(found) if *found != object => findings.push(AuditFinding::Corrupt {
                path: object.path.clone(),
                expected_size: object.size,
                actual_size: found.size,
                expected_sha256: object.sha256.clone(),
                actual_sha256: found.sha256.clone(),
            }),
            Some(_) => {}
        }
    }
    for object in actual.values() {
        if !expected.contains_key(object.path.as_str()) {
            findings.push(AuditFinding::Orphan {
                path: object.path.clone(),
                actual_size: object.size,
                actual_sha256: object.sha256.clone(),
            });
        }
    }

    Ok(AuditReport {
        schema_version: BACKUP_SCHEMA_VERSION,
        tenant: manifest.tenant.clone(),
        cut_id: manifest.cut_id.clone(),
        manifest_sha256: digest(&manifest_bytes(manifest)?),
        read_only: true,
        findings,
    })
}

/// Restore or resume one tenant into a dedicated empty prefix.
///
/// No object is overwritten or deleted. Existing matching objects make a
/// retry cheap; corrupt or unlisted target objects stop the restore before a
/// write occurs.
///
/// # Errors
///
/// Returns an error when the backup is incomplete or corrupt, the target
/// contains conflicting content, or either store operation fails.
pub async fn restore_backup(
    backup: Arc<dyn ObjectStore>,
    target: Arc<dyn ObjectStore>,
) -> Result<RestoreReport, RecoveryError> {
    let manifest = load_backup_manifest(backup.as_ref()).await?;
    let source_audit = audit_backup(backup.as_ref(), &manifest).await?;
    refuse_unsafe("restore source", &source_audit, false)?;
    let before = audit_recovery_target(target.as_ref(), &manifest).await?;
    refuse_unsafe("restore target", &before, true)?;
    let (created, already_present) =
        copy_manifest_objects(backup.as_ref(), target.as_ref(), &manifest).await?;
    let audit = audit_recovery_target(target.as_ref(), &manifest).await?;
    refuse_unsafe("restore target", &audit, false)?;
    Ok(RestoreReport {
        created,
        already_present,
        audit,
    })
}

/// Repair a target from a completed backup under an exact, offline scope.
///
/// Missing objects use create-if-absent writes. Corrupt objects use conditional
/// replacement against the version audited by this pass. Orphans are deleted
/// only when the caller explicitly enables deletion and attests that writers
/// are offline.
///
/// # Errors
///
/// Returns an error before mutation when the scope does not exactly match the
/// backup or does not authorize every reported damage class.
pub async fn repair_from_backup(
    backup: Arc<dyn ObjectStore>,
    target: Arc<dyn ObjectStore>,
    scope: RepairScope,
) -> Result<RepairReport, RecoveryError> {
    let manifest = load_backup_manifest(backup.as_ref()).await?;
    let expected_digest = digest(&manifest_bytes(&manifest)?);
    if !scope.offline
        || scope.tenant != manifest.tenant
        || scope.cut_id != manifest.cut_id
        || scope.manifest_sha256 != expected_digest
    {
        return Err(RecoveryError::InvalidManifest(
            "repair scope must attest offline writers and exactly match tenant, cut, and manifest"
                .into(),
        ));
    }
    let source_audit = audit_backup(backup.as_ref(), &manifest).await?;
    refuse_unsafe("repair source", &source_audit, false)?;
    let before = audit_recovery_target(target.as_ref(), &manifest).await?;
    let unauthorized = before.findings.iter().any(|finding| match finding {
        AuditFinding::Missing { .. } => false,
        AuditFinding::Corrupt { .. } => !scope.replace_corrupt,
        AuditFinding::Orphan { .. }
        | AuditFinding::Stale { .. }
        | AuditFinding::SplitBrain { .. } => !scope.delete_orphans,
    });
    if unauthorized {
        return Err(RecoveryError::UnsafeTarget {
            operation: "repair",
            report: Box::new(before),
        });
    }

    let mut actions = Vec::new();
    for finding in &before.findings {
        let (operation, path) = match finding {
            AuditFinding::Missing { path } => {
                let object = manifest_object(&manifest, path)?;
                let bytes = checked_source_object(backup.as_ref(), object).await?;
                put_create_or_equal(target.as_ref(), &ObjectPath::from(path.clone()), &bytes)
                    .await?;
                ("create", path)
            }
            AuditFinding::Corrupt { path, .. } => {
                let object = manifest_object(&manifest, path)?;
                let bytes = checked_source_object(backup.as_ref(), object).await?;
                let object_path = ObjectPath::from(path.clone());
                let found = target.head(&object_path).await?;
                target
                    .put_opts(
                        &object_path,
                        PutPayload::from(bytes),
                        PutOptions::from(PutMode::Update(UpdateVersion {
                            e_tag: found.e_tag,
                            version: found.version,
                        })),
                    )
                    .await?;
                ("replace", path)
            }
            AuditFinding::Orphan { path, .. }
            | AuditFinding::Stale { path, .. }
            | AuditFinding::SplitBrain { path, .. } => {
                target.delete(&ObjectPath::from(path.clone())).await?;
                ("delete", path)
            }
        };
        actions.push(RepairAction {
            operation: operation.into(),
            path: path.clone(),
        });
    }
    let after = audit_recovery_target(target.as_ref(), &manifest).await?;
    refuse_unsafe("repair target", &after, false)?;
    Ok(RepairReport {
        scope,
        before,
        actions,
        after,
    })
}

fn manifest_object<'a>(
    manifest: &'a BackupManifest,
    path: &str,
) -> Result<&'a BackupObject, RecoveryError> {
    manifest
        .objects
        .iter()
        .find(|object| object.path == path)
        .ok_or_else(|| RecoveryError::InvalidManifest(format!("manifest has no object `{path}`")))
}

async fn checked_source_object(
    source: &dyn ObjectStore,
    object: &BackupObject,
) -> Result<Vec<u8>, RecoveryError> {
    let bytes = source
        .get(&ObjectPath::from(object.path.clone()))
        .await?
        .bytes()
        .await?
        .to_vec();
    if u64::try_from(bytes.len()).ok() != Some(object.size) || digest(&bytes) != object.sha256 {
        return Err(RecoveryError::InvalidManifest(format!(
            "source object `{}` changed after the cut was recorded",
            object.path
        )));
    }
    Ok(bytes)
}

async fn load_backup_manifest_optional(
    backup: &dyn ObjectStore,
) -> Result<Option<BackupManifest>, RecoveryError> {
    let result = match backup.get(&ObjectPath::from(BACKUP_MANIFEST_PATH)).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let bytes = result.bytes().await?;
    let manifest = serde_json::from_slice::<BackupManifest>(&bytes)?;
    validate_manifest(&manifest)?;
    Ok(Some(manifest))
}

async fn inventory(
    store: &dyn ObjectStore,
    ignore_completion_marker: bool,
) -> Result<Vec<BackupObject>, RecoveryError> {
    let mut paths = store
        .list(None)
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await?;
    paths.sort();
    let mut objects = Vec::with_capacity(paths.len());
    for path in paths {
        if ignore_completion_marker && path.as_ref() == BACKUP_MANIFEST_PATH {
            continue;
        }
        let bytes = store.get(&path).await?.bytes().await?;
        objects.push(BackupObject {
            path: path.to_string(),
            size: u64::try_from(bytes.len()).map_err(|_| {
                RecoveryError::InvalidManifest(format!("object `{path}` is too large to inventory"))
            })?,
            sha256: digest(&bytes),
        });
    }
    Ok(objects)
}

async fn copy_manifest_objects(
    source: &dyn ObjectStore,
    target: &dyn ObjectStore,
    manifest: &BackupManifest,
) -> Result<(usize, usize), RecoveryError> {
    let mut created = 0;
    let mut already_present = 0;
    for object in &manifest.objects {
        let path = ObjectPath::from(object.path.clone());
        let bytes = source.get(&path).await?.bytes().await?;
        if u64::try_from(bytes.len()).ok() != Some(object.size) || digest(&bytes) != object.sha256 {
            return Err(RecoveryError::InvalidManifest(format!(
                "source object `{}` changed after the cut was recorded",
                object.path
            )));
        }
        if put_create_or_equal(target, &path, &bytes).await? {
            created += 1;
        } else {
            already_present += 1;
        }
    }
    Ok((created, already_present))
}

async fn put_create_or_equal(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    bytes: &[u8],
) -> Result<bool, RecoveryError> {
    match store
        .put_opts(
            path,
            PutPayload::from(bytes.to_vec()),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = store.get(path).await?.bytes().await?;
            if existing.as_ref() == bytes {
                Ok(false)
            } else {
                Err(RecoveryError::InvalidManifest(format!(
                    "target object `{path}` already exists with different bytes"
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn refuse_unsafe(
    operation: &'static str,
    report: &AuditReport,
    allow_missing: bool,
) -> Result<(), RecoveryError> {
    let unsafe_finding = report
        .findings
        .iter()
        .any(|finding| !allow_missing || !matches!(finding, AuditFinding::Missing { .. }));
    if unsafe_finding {
        return Err(RecoveryError::UnsafeTarget {
            operation,
            report: Box::new(report.clone()),
        });
    }
    Ok(())
}

fn validate_manifest(manifest: &BackupManifest) -> Result<(), RecoveryError> {
    if manifest.schema_version != BACKUP_SCHEMA_VERSION {
        return Err(RecoveryError::InvalidManifest(format!(
            "schema version {} is unsupported",
            manifest.schema_version
        )));
    }
    if manifest.cut_id.trim().is_empty() {
        return Err(RecoveryError::InvalidManifest("cut_id is empty".into()));
    }
    if manifest.broker_snapshot.id.trim().is_empty()
        || manifest.broker_snapshot.sha256.len() != 64
        || !manifest
            .broker_snapshot
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(RecoveryError::InvalidManifest(
            "broker snapshot identity must include an id and SHA-256".into(),
        ));
    }
    if manifest.wal_offsets.is_empty() {
        return Err(RecoveryError::InvalidManifest(
            "the consistent cut records no WAL offsets".into(),
        ));
    }
    if manifest
        .wal_offsets
        .windows(2)
        .any(|pair| (&pair[0].topic, pair[0].partition) >= (&pair[1].topic, pair[1].partition))
        || manifest
            .wal_offsets
            .iter()
            .any(|offset| offset.topic.is_empty() || offset.partition < 0 || offset.next_offset < 0)
    {
        return Err(RecoveryError::InvalidManifest(
            "WAL offsets must be sorted, unique, and non-negative".into(),
        ));
    }
    if manifest
        .objects
        .windows(2)
        .any(|pair| pair[0].path >= pair[1].path)
        || manifest.objects.iter().any(|object| {
            object.path.is_empty()
                || object.path == BACKUP_MANIFEST_PATH
                || object.sha256.len() != 64
        })
    {
        return Err(RecoveryError::InvalidManifest(
            "objects must be sorted, unique, and checksummed".into(),
        ));
    }
    Ok(())
}

fn manifest_bytes(manifest: &BackupManifest) -> Result<Vec<u8>, RecoveryError> {
    Ok(serde_json::to_vec_pretty(manifest)?)
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use object_store::{ObjectStoreExt as _, memory::InMemory};

    use super::*;

    fn tenant() -> TenantId {
        TenantId::new("tenant-a").unwrap()
    }

    fn offsets() -> Vec<WalOffset> {
        vec![WalOffset {
            topic: "krabka.metrics.wal".into(),
            partition: 0,
            next_offset: 42,
        }]
    }

    fn snapshot() -> BrokerSnapshot {
        BrokerSnapshot {
            id: "broker-snapshot-1".into(),
            sha256: "a".repeat(64),
        }
    }

    async fn put(store: &Arc<dyn ObjectStore>, path: &str, bytes: &[u8]) {
        store
            .put(&ObjectPath::from(path), bytes.to_vec().into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn backup_and_restore_are_resumable_and_idempotent() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&source, "blocks/a.parquet", b"block").await;
        put(&source, "indexes/a.index", b"index").await;
        put(&source, "symbols/a.sym", b"symbols").await;
        put(&source, "delete/1.json", b"delete").await;

        let first = create_backup(
            source.clone(),
            backup.clone(),
            tenant(),
            "cut-1".into(),
            snapshot(),
            offsets(),
        )
        .await
        .unwrap();
        check!(first.created == 4);
        assert!(first.audit.is_clean());

        let repeated = create_backup(
            source,
            backup.clone(),
            tenant(),
            "cut-1".into(),
            snapshot(),
            offsets(),
        )
        .await
        .unwrap();
        check!(repeated.created == 0);
        check!(repeated.already_present == 4);

        let restored = restore_backup(backup.clone(), target.clone())
            .await
            .unwrap();
        check!(restored.created == 4);
        assert!(restored.audit.is_clean());
        let repeated = restore_backup(backup, target).await.unwrap();
        check!(repeated.created == 0);
        check!(repeated.already_present == 4);
    }

    #[tokio::test]
    async fn audit_reports_missing_corrupt_and_orphan_objects_stably() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&source, "a", b"one").await;
        put(&source, "b", b"two").await;
        create_backup(
            source,
            backup.clone(),
            tenant(),
            "cut-1".into(),
            snapshot(),
            offsets(),
        )
        .await
        .unwrap();
        let manifest = load_backup_manifest(backup.as_ref()).await.unwrap();
        backup.delete(&ObjectPath::from("a")).await.unwrap();
        put(&backup, "b", b"changed").await;
        put(&backup, "c", b"orphan").await;

        let report = audit_backup(backup.as_ref(), &manifest).await.unwrap();
        check!(
            report
                .findings
                .iter()
                .map(|finding| match finding {
                    AuditFinding::Missing { path } => ("missing", path.as_str()),
                    AuditFinding::Corrupt { path, .. } => ("corrupt", path.as_str()),
                    AuditFinding::Orphan { path, .. } => ("orphan", path.as_str()),
                    AuditFinding::Stale { path, .. } => ("stale", path.as_str()),
                    AuditFinding::SplitBrain { path, .. } => ("split_brain", path.as_str()),
                })
                .collect::<Vec<_>>()
                == [("missing", "a"), ("corrupt", "b"), ("orphan", "c")]
        );
        assert!(report.manifest_sha256.len() == 64);
    }

    #[tokio::test]
    async fn restore_refuses_a_mixed_target_before_copying() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&source, "a", b"one").await;
        create_backup(
            source,
            backup.clone(),
            tenant(),
            "cut-1".into(),
            snapshot(),
            offsets(),
        )
        .await
        .unwrap();
        put(&target, "unexpected", b"live").await;

        let error = restore_backup(backup, target.clone()).await.unwrap_err();
        assert!(matches!(error, RecoveryError::UnsafeTarget { .. }));
        assert!(target.get(&ObjectPath::from("a")).await.is_err());
    }

    #[tokio::test]
    async fn scoped_repair_handles_every_damage_class_and_is_resumable() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&source, "a", b"one").await;
        put(&source, "b", b"two").await;
        create_backup(
            source,
            backup.clone(),
            tenant(),
            "cut-1".into(),
            snapshot(),
            offsets(),
        )
        .await
        .unwrap();
        put(&target, "b", b"corrupt").await;
        put(&target, "orphan", b"old").await;

        let manifest = load_backup_manifest(backup.as_ref()).await.unwrap();
        let mut stale = manifest.clone();
        stale.cut_id = "cut-0".into();
        put(
            &target,
            BACKUP_MANIFEST_PATH,
            &manifest_bytes(&stale).unwrap(),
        )
        .await;
        let before = audit_recovery_target(target.as_ref(), &manifest)
            .await
            .unwrap();
        check!(
            before
                .findings
                .iter()
                .any(|finding| matches!(finding, AuditFinding::Missing { .. }))
        );
        check!(
            before
                .findings
                .iter()
                .any(|finding| matches!(finding, AuditFinding::Corrupt { .. }))
        );
        check!(
            before
                .findings
                .iter()
                .any(|finding| matches!(finding, AuditFinding::Orphan { .. }))
        );
        check!(
            before
                .findings
                .iter()
                .any(|finding| matches!(finding, AuditFinding::Stale { .. }))
        );

        let scope = RepairScope {
            tenant: tenant(),
            cut_id: manifest.cut_id.clone(),
            manifest_sha256: before.manifest_sha256.clone(),
            offline: true,
            replace_corrupt: true,
            delete_orphans: true,
        };
        let repaired = repair_from_backup(backup.clone(), target.clone(), scope.clone())
            .await
            .unwrap();
        assert!(repaired.after.is_clean());
        check!(repaired.actions.len() == 4);
        let repeated = repair_from_backup(backup.clone(), target, scope)
            .await
            .unwrap();
        check!(repeated.actions.is_empty());

        let split_target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut split = manifest.clone();
        split.tenant = TenantId::new("tenant-b").unwrap();
        put(
            &split_target,
            BACKUP_MANIFEST_PATH,
            &manifest_bytes(&split).unwrap(),
        )
        .await;
        let split_report = audit_recovery_target(split_target.as_ref(), &manifest)
            .await
            .unwrap();
        assert!(
            split_report
                .findings
                .iter()
                .any(|finding| matches!(finding, AuditFinding::SplitBrain { .. }))
        );
    }
}
