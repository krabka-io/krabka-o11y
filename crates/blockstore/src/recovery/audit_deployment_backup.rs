use super::{
    Arc, BTreeMap, DeploymentAuditReport, ObjectStore, RecoveryError, audit_backup,
    load_backup_manifest_with_digest, load_cut, part_store, refuse_unplanned_objects,
};

/// Verifies a deployment backup set without changing it.
///
/// The set must hold a sealed [`DeploymentCut`](super::DeploymentCut), one
/// part for each part the cut names, and nothing else. Each part manifest
/// must have the digest that the cut recorded, the cut id of the cut, and the
/// WAL offsets of the cut. The report holds the object audit of every part;
/// a part with a finding is damaged.
///
/// # Errors
///
/// Returns [`RecoveryError::MixedSet`] when the cut is absent, a part is
/// absent or belongs to another cut, or an object belongs to no part. Returns
/// [`RecoveryError::InvalidManifest`] for a malformed or unsupported cut or
/// part manifest.
pub async fn audit_deployment_backup(
    backup: &Arc<dyn ObjectStore>,
) -> Result<DeploymentAuditReport, RecoveryError> {
    let (cut, cut_sha256) = load_cut(backup.as_ref()).await?;
    let names = cut
        .parts
        .iter()
        .map(|part| part.name.clone())
        .collect::<Vec<_>>();
    refuse_unplanned_objects(backup.as_ref(), &names).await?;

    let mut parts = BTreeMap::new();
    for expected in &cut.parts {
        let store = part_store(backup, &expected.name);
        let Some((manifest, manifest_sha256)) = load_backup_manifest_with_digest(&store).await?
        else {
            return Err(RecoveryError::MixedSet(format!(
                "part `{}` has no manifest",
                expected.name
            )));
        };
        if manifest_sha256 != expected.manifest_sha256 {
            return Err(RecoveryError::MixedSet(format!(
                "part `{}` manifest digest {manifest_sha256} differs from the cut",
                expected.name
            )));
        }
        if manifest.cut_id != cut.cut_id
            || manifest.part.as_deref() != Some(expected.name.as_str())
            || manifest.wal_offsets != cut.broker.wal_offsets
        {
            return Err(RecoveryError::MixedSet(format!(
                "part `{}` belongs to another cut",
                expected.name
            )));
        }
        parts.insert(
            expected.name.clone(),
            audit_backup(&store, &manifest).await?,
        );
    }
    Ok(DeploymentAuditReport {
        cut,
        cut_sha256,
        parts,
    })
}
