//! Checksummed object-store backup and recovery, for one prefix or for a
//! whole deployment.
//!
//! A *part* is one object-store prefix or one local state directory, copied
//! with create-if-absent semantics and closed by a [`BackupManifest`] that
//! names every object with its size and SHA-256.
//!
//! A *deployment cut* binds one part for each store of a deployment to one
//! [`BrokerSnapshot`]. The broker snapshot records the next offset of every
//! WAL and state partition, and the committed offset of every consumer group.
//! [`backup_deployment`] copies the parts and writes the [`DeploymentCut`]
//! last, after a second broker capture shows that no writer moved during the
//! copy. [`restore_deployment_backup`] refuses a partial or mixed set, and a
//! restored broker that is not the recorded snapshot, before it writes one
//! object.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, path::Path as ObjectPath,
    prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::TenantId;

/// Completion marker written last in every backup set.
pub const BACKUP_MANIFEST_PATH: &str = ".krabka-recovery/manifest.json";
/// The manifest version that readers accept as the previous release.
const LEGACY_BACKUP_SCHEMA_VERSION: u32 = 1;
/// The manifest version that writers stamp.
const BACKUP_SCHEMA_VERSION: u32 = 2;
/// Completion record of a deployment backup set, relative to its root.
pub const CUT_MANIFEST_PATH: &str = "krabka-recovery/cut.json";
/// The prefix under the backup root that holds one directory per part.
const PARTS_PREFIX: &str = "parts";
/// The deployment cut version that writers stamp and readers accept.
const CUT_SCHEMA_VERSION: u32 = 1;

/// The next offset to consume for one WAL partition at the backup cut.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WalOffset {
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

/// One immutable object in a backup set.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BackupObject {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

/// Identity and consistent-cut metadata for one backup part.
///
/// Version 1 manifests always name a tenant. Version 2 adds `part`, the name
/// of the part inside a [`DeploymentCut`]. A version 2 manifest names a
/// tenant, a part, or both.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupManifest {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part: Option<String>,
    pub cut_id: String,
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
}

/// Read-only comparison of one object-store prefix with its backup manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuditReport {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part: Option<String>,
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

/// Backup, audit, or restore failure.
#[derive(Debug, Error)]
#[non_exhaustive]
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
    /// The backup set is missing a part, holds a part of another cut, or holds
    /// an object that no part names.
    #[error("backup set is partial or mixed: {0}")]
    MixedSet(String),
    /// The broker could not be read.
    #[error("broker state could not be read: {0}")]
    Broker(String),
    /// The broker does not hold the state that the cut recorded.
    #[error("{operation} refused because the broker state differs from the cut")]
    BrokerMismatch {
        operation: &'static str,
        findings: Vec<BrokerFinding>,
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
    wal_offsets: Vec<WalOffset>,
) -> Result<RestoreReport, RecoveryError> {
    create_part_backup(
        source.as_ref(),
        backup.as_ref(),
        Some(tenant),
        None,
        cut_id,
        wal_offsets,
    )
    .await
}

async fn create_part_backup(
    source: &dyn ObjectStore,
    backup: &dyn ObjectStore,
    tenant: Option<TenantId>,
    part: Option<String>,
    cut_id: String,
    mut wal_offsets: Vec<WalOffset>,
) -> Result<RestoreReport, RecoveryError> {
    wal_offsets.sort();
    let manifest = BackupManifest {
        schema_version: BACKUP_SCHEMA_VERSION,
        tenant,
        part,
        cut_id,
        wal_offsets,
        objects: inventory(source, true).await?,
    };
    validate_manifest(&manifest)?;

    if let Some(existing) = load_backup_manifest_optional(backup).await?
        && existing != manifest
    {
        return Err(RecoveryError::InvalidManifest(
            "the backup prefix already contains a different completed cut".into(),
        ));
    }

    let before = audit_backup(backup, &manifest).await?;
    refuse_unsafe("backup", &before, true)?;
    let (created, already_present) = copy_manifest_objects(source, backup, &manifest).await?;
    let manifest_bytes = manifest_bytes(&manifest)?;
    let _ = put_create_or_equal(
        backup,
        &ObjectPath::from(BACKUP_MANIFEST_PATH),
        &manifest_bytes,
    )
    .await?;
    let audit = audit_backup(backup, &manifest).await?;
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
    let actual = inventory(store, ignore_completion_marker).await?;
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
        schema_version: manifest.schema_version,
        tenant: manifest.tenant.clone(),
        part: manifest.part.clone(),
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
    copy_restored_part(backup.as_ref(), target.as_ref(), &manifest).await
}

async fn copy_restored_part(
    backup: &dyn ObjectStore,
    target: &dyn ObjectStore,
    manifest: &BackupManifest,
) -> Result<RestoreReport, RecoveryError> {
    let (created, already_present) = copy_manifest_objects(backup, target, manifest).await?;
    let audit = audit_recovery_target(target, manifest).await?;
    refuse_unsafe("restore target", &audit, false)?;
    Ok(RestoreReport {
        created,
        already_present,
        audit,
    })
}

async fn load_backup_manifest_optional(
    backup: &dyn ObjectStore,
) -> Result<Option<BackupManifest>, RecoveryError> {
    Ok(load_backup_manifest_with_digest(backup)
        .await?
        .map(|(manifest, _)| manifest))
}

/// The manifest and the SHA-256 of its stored bytes, or `None` when the part
/// has no completion marker.
async fn load_backup_manifest_with_digest(
    backup: &dyn ObjectStore,
) -> Result<Option<(BackupManifest, String)>, RecoveryError> {
    let Some(bytes) = get_optional(backup, BACKUP_MANIFEST_PATH).await? else {
        return Ok(None);
    };
    let manifest = serde_json::from_slice::<BackupManifest>(&bytes)?;
    validate_manifest(&manifest)?;
    Ok(Some((manifest, digest(&bytes))))
}

async fn get_optional(
    store: &dyn ObjectStore,
    path: &str,
) -> Result<Option<Vec<u8>>, RecoveryError> {
    match store.get(&ObjectPath::from(path)).await {
        Ok(result) => Ok(Some(result.bytes().await?.to_vec())),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
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
    match manifest.schema_version {
        LEGACY_BACKUP_SCHEMA_VERSION => {
            if manifest.tenant.is_none() || manifest.part.is_some() {
                return Err(RecoveryError::InvalidManifest(
                    "a version 1 manifest names one tenant and no part".into(),
                ));
            }
        }
        BACKUP_SCHEMA_VERSION => {
            if manifest.tenant.is_none() && manifest.part.is_none() {
                return Err(RecoveryError::InvalidManifest(
                    "a version 2 manifest names a tenant, a part, or both".into(),
                ));
            }
            if let Some(part) = &manifest.part {
                validate_part_name(part)?;
            }
        }
        version => {
            return Err(RecoveryError::InvalidManifest(format!(
                "schema version {version} is unsupported"
            )));
        }
    }
    if manifest.cut_id.trim().is_empty() {
        return Err(RecoveryError::InvalidManifest("cut_id is empty".into()));
    }
    validate_wal_offsets(&manifest.wal_offsets)?;
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

fn validate_wal_offsets(wal_offsets: &[WalOffset]) -> Result<(), RecoveryError> {
    if wal_offsets.is_empty() {
        return Err(RecoveryError::InvalidManifest(
            "the consistent cut records no WAL offsets".into(),
        ));
    }
    if wal_offsets
        .windows(2)
        .any(|pair| (&pair[0].topic, pair[0].partition) >= (&pair[1].topic, pair[1].partition))
        || wal_offsets
            .iter()
            .any(|offset| offset.topic.is_empty() || offset.partition < 0 || offset.next_offset < 0)
    {
        return Err(RecoveryError::InvalidManifest(
            "WAL offsets must be sorted, unique, and non-negative".into(),
        ));
    }
    Ok(())
}

/// The store of one part under a deployment backup root.
fn part_store(backup: &Arc<dyn ObjectStore>, name: &str) -> PrefixStore<Arc<dyn ObjectStore>> {
    PrefixStore::new(Arc::clone(backup), format!("{PARTS_PREFIX}/{name}"))
}

/// Refuses a backup root that holds an object outside the named parts.
async fn refuse_unplanned_objects(
    backup: &dyn ObjectStore,
    names: &[String],
) -> Result<(), RecoveryError> {
    let prefixes = names
        .iter()
        .map(|name| format!("{PARTS_PREFIX}/{name}/"))
        .collect::<Vec<_>>();
    let mut paths = backup
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await?;
    paths.sort();
    if let Some(path) = paths.iter().find(|path| {
        path.as_str() != CUT_MANIFEST_PATH
            && !prefixes.iter().any(|prefix| path.starts_with(prefix))
    }) {
        return Err(RecoveryError::MixedSet(format!(
            "object `{path}` belongs to no part of the cut"
        )));
    }
    Ok(())
}

async fn load_cut(backup: &dyn ObjectStore) -> Result<(DeploymentCut, String), RecoveryError> {
    let bytes = get_optional(backup, CUT_MANIFEST_PATH)
        .await?
        .ok_or_else(|| RecoveryError::MixedSet("the backup set has no sealed cut".into()))?;
    let cut = serde_json::from_slice::<DeploymentCut>(&bytes)?;
    validate_cut(&cut)?;
    Ok((cut, digest(&bytes)))
}

fn validate_part_name(name: &str) -> Result<(), RecoveryError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(RecoveryError::InvalidManifest(format!(
            "part name `{name}` must be 1 to 64 lowercase letters, digits, or `-`"
        )))
    }
}

fn validate_plan(plan: &DeploymentBackupPlan) -> Result<(), RecoveryError> {
    if plan.cut_id.trim().is_empty() || plan.broker_capture.trim().is_empty() {
        return Err(RecoveryError::InvalidManifest(
            "a deployment cut needs a cut id and a broker capture".into(),
        ));
    }
    if plan.parts.is_empty() {
        return Err(RecoveryError::InvalidManifest(
            "a deployment cut needs at least one part".into(),
        ));
    }
    let mut names = BTreeSet::new();
    for part in &plan.parts {
        validate_part_name(&part.name)?;
        if !names.insert(part.name.as_str()) {
            return Err(RecoveryError::InvalidManifest(format!(
                "part `{}` appears more than once",
                part.name
            )));
        }
    }
    Ok(())
}

fn validate_cut(cut: &DeploymentCut) -> Result<(), RecoveryError> {
    if cut.schema_version != CUT_SCHEMA_VERSION {
        return Err(RecoveryError::InvalidManifest(format!(
            "deployment cut schema version {} is unsupported",
            cut.schema_version
        )));
    }
    if cut.cut_id.trim().is_empty() || cut.broker_capture.trim().is_empty() {
        return Err(RecoveryError::InvalidManifest(
            "a deployment cut needs a cut id and a broker capture".into(),
        ));
    }
    validate_wal_offsets(&cut.broker.wal_offsets)?;
    if cut.broker.group_offsets.windows(2).any(|pair| {
        (&pair[0].group, &pair[0].topic, pair[0].partition)
            >= (&pair[1].group, &pair[1].topic, pair[1].partition)
    }) || cut
        .broker
        .group_offsets
        .iter()
        .any(|offset| offset.group.is_empty() || offset.partition < 0 || offset.next_offset < 0)
    {
        return Err(RecoveryError::InvalidManifest(
            "group offsets must be sorted, unique, and non-negative".into(),
        ));
    }
    if cut.parts.is_empty()
        || cut
            .parts
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
        || cut
            .parts
            .iter()
            .any(|part| part.manifest_sha256.len() != 64)
    {
        return Err(RecoveryError::InvalidManifest(
            "cut parts must be present, sorted, unique, and checksummed".into(),
        ));
    }
    for part in &cut.parts {
        validate_part_name(&part.name)?;
    }
    Ok(())
}

/// Every difference between two broker snapshots, in a stable order.
fn compare_snapshots(expected: &BrokerSnapshot, actual: &BrokerSnapshot) -> Vec<BrokerFinding> {
    let wal = |snapshot: &BrokerSnapshot| {
        snapshot
            .wal_offsets
            .iter()
            .map(|offset| ((offset.topic.clone(), offset.partition), offset.next_offset))
            .collect::<BTreeMap<_, _>>()
    };
    let groups = |snapshot: &BrokerSnapshot| {
        snapshot
            .group_offsets
            .iter()
            .map(|offset| {
                (
                    (offset.group.clone(), offset.topic.clone(), offset.partition),
                    offset.next_offset,
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let (expected_wal, actual_wal) = (wal(expected), wal(actual));
    let (expected_groups, actual_groups) = (groups(expected), groups(actual));
    let mut findings = Vec::new();
    for key in expected_wal
        .keys()
        .chain(actual_wal.keys())
        .collect::<BTreeSet<_>>()
    {
        let (expected, actual) = (expected_wal.get(key), actual_wal.get(key));
        if expected != actual {
            findings.push(BrokerFinding::WalOffset {
                topic: key.0.clone(),
                partition: key.1,
                expected: expected.copied(),
                actual: actual.copied(),
            });
        }
    }
    for key in expected_groups
        .keys()
        .chain(actual_groups.keys())
        .collect::<BTreeSet<_>>()
    {
        let (expected, actual) = (expected_groups.get(key), actual_groups.get(key));
        if expected != actual {
            findings.push(BrokerFinding::GroupOffset {
                group: key.0.clone(),
                topic: key.1.clone(),
                partition: key.2,
                expected: expected.copied(),
                actual: actual.copied(),
            });
        }
    }
    findings
}

/// Every partition where a drained group has records after its committed
/// offset.
async fn check_drained_groups(
    broker: &dyn BrokerState,
    snapshot: &BrokerSnapshot,
    drained: &[DrainedGroup],
) -> Result<Vec<BrokerFinding>, RecoveryError> {
    let committed = snapshot
        .group_offsets
        .iter()
        .map(|offset| {
            (
                (
                    offset.group.as_str(),
                    offset.topic.as_str(),
                    offset.partition,
                ),
                offset.next_offset,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut findings = Vec::new();
    for group in drained {
        let partitions = snapshot
            .wal_offsets
            .iter()
            .filter(|offset| offset.topic == group.topic)
            .collect::<Vec<_>>();
        if partitions.is_empty() {
            findings.push(BrokerFinding::WalOffset {
                topic: group.topic.clone(),
                partition: 0,
                expected: None,
                actual: None,
            });
        }
        for offset in partitions {
            let found = committed
                .get(&(
                    group.group.as_str(),
                    offset.topic.as_str(),
                    offset.partition,
                ))
                .copied();
            // A group with no committed offset reads from the start.
            let from = found.unwrap_or(0);
            if from == offset.next_offset {
                continue;
            }
            let pending = if from > offset.next_offset {
                1
            } else {
                broker
                    .records_between(&offset.topic, offset.partition, from, offset.next_offset)
                    .await
                    .map_err(RecoveryError::Broker)?
            };
            if pending > 0 {
                findings.push(BrokerFinding::UndrainedGroup {
                    group: group.group.clone(),
                    topic: offset.topic.clone(),
                    partition: offset.partition,
                    committed: found,
                    wal_next_offset: offset.next_offset,
                });
            }
        }
    }
    Ok(findings)
}

fn refuse_broker_findings(
    operation: &'static str,
    findings: Vec<BrokerFinding>,
) -> Result<(), RecoveryError> {
    if findings.is_empty() {
        Ok(())
    } else {
        Err(RecoveryError::BrokerMismatch {
            operation,
            findings,
        })
    }
}

fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, RecoveryError> {
    Ok(serde_json::to_vec_pretty(value)?)
}

fn manifest_bytes(manifest: &BackupManifest) -> Result<Vec<u8>, RecoveryError> {
    json_bytes(manifest)
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use object_store::memory::InMemory;

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
            offsets(),
        )
        .await
        .unwrap();
        check!(first.created == 4);
        assert!(first.audit.is_clean());

        let repeated = create_backup(source, backup.clone(), tenant(), "cut-1".into(), offsets())
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
        create_backup(source, backup.clone(), tenant(), "cut-1".into(), offsets())
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
        create_backup(source, backup.clone(), tenant(), "cut-1".into(), offsets())
            .await
            .unwrap();
        put(&target, "unexpected", b"live").await;

        let error = restore_backup(backup, target.clone()).await.unwrap_err();
        assert!(matches!(error, RecoveryError::UnsafeTarget { .. }));
        assert!(target.get(&ObjectPath::from("a")).await.is_err());
    }
}

mod audit_deployment_backup;
mod backup_deployment;
mod broker_finding;
mod broker_snapshot;
mod broker_state;
mod cut_part;
mod deployment_audit_report;
mod deployment_backup_plan;
mod deployment_backup_report;
mod deployment_cut;
mod deployment_part;
mod deployment_restore_report;
mod drained_group;
mod group_offset;
mod restore_deployment_backup;

pub use audit_deployment_backup::audit_deployment_backup;
pub use backup_deployment::backup_deployment;
pub use broker_finding::BrokerFinding;
pub use broker_snapshot::BrokerSnapshot;
pub use broker_state::BrokerState;
pub use cut_part::CutPart;
pub use deployment_audit_report::DeploymentAuditReport;
pub use deployment_backup_plan::DeploymentBackupPlan;
pub use deployment_backup_report::DeploymentBackupReport;
pub use deployment_cut::DeploymentCut;
pub use deployment_part::DeploymentPart;
pub use deployment_restore_report::DeploymentRestoreReport;
pub use drained_group::DrainedGroup;
pub use group_offset::GroupOffset;
pub use restore_deployment_backup::restore_deployment_backup;
