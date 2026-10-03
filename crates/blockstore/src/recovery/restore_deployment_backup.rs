use super::{
    Arc, BTreeMap, BTreeSet, BrokerState, COPY_CHUNK_BYTES, DeploymentPart,
    DeploymentRestoreReport, ObjectStore, RecoveryError, audit_deployment_backup,
    audit_recovery_target, compare_snapshots, copy_restored_part, load_backup_manifest, part_store,
    refuse_broker_findings, refuse_unsafe,
};

/// Restores every part of a deployment backup set into empty targets.
///
/// Restore the broker snapshot first and start no writer. This function then
/// refuses, before it writes one object:
///
/// - a set that [`audit_deployment_backup`] refuses, or a part with damage;
/// - targets that do not name exactly the parts of the cut;
/// - a broker whose partition offsets or committed group offsets differ from
///   the cut, which is a broker snapshot of another time;
/// - a target that holds an object the part does not hold.
///
/// No object is overwritten or deleted, so a retry resumes.
///
/// # Errors
///
/// Returns [`RecoveryError::MixedSet`], [`RecoveryError::BrokerMismatch`], or
/// [`RecoveryError::UnsafeTarget`] for the conditions above, and an
/// object-store error when a copy fails.
pub async fn restore_deployment_backup(
    backup: &Arc<dyn ObjectStore>,
    broker: &dyn BrokerState,
    targets: &[DeploymentPart],
) -> Result<DeploymentRestoreReport, RecoveryError> {
    let audit = audit_deployment_backup(backup).await?;
    for report in audit.parts.values() {
        refuse_unsafe("restore source", report, false)?;
    }
    let expected = audit
        .cut
        .parts
        .iter()
        .map(|part| part.name.as_str())
        .collect::<BTreeSet<_>>();
    let actual = targets
        .iter()
        .map(|part| part.name.as_str())
        .collect::<BTreeSet<_>>();
    if expected != actual || actual.len() != targets.len() {
        return Err(RecoveryError::MixedSet(format!(
            "the restore names parts {actual:?}, and the cut holds parts {expected:?}"
        )));
    }

    let restored_broker = broker
        .capture()
        .await
        .map_err(RecoveryError::Broker)?
        .sorted();
    refuse_broker_findings(
        "restore",
        compare_snapshots(&audit.cut.broker, &restored_broker),
    )?;

    let mut manifests = Vec::with_capacity(targets.len());
    for target in targets {
        let source = part_store(backup, &target.name);
        let manifest = load_backup_manifest(&source).await?;
        let before = audit_recovery_target(target.store.as_ref(), &manifest).await?;
        refuse_unsafe("restore target", &before, true)?;
        manifests.push((source, manifest));
    }

    let mut parts = BTreeMap::new();
    for (target, (source, manifest)) in targets.iter().zip(&manifests) {
        let report =
            copy_restored_part(source, target.store.as_ref(), manifest, COPY_CHUNK_BYTES).await?;
        parts.insert(target.name.clone(), report);
    }
    Ok(DeploymentRestoreReport {
        cut: audit.cut,
        cut_sha256: audit.cut_sha256,
        broker: restored_broker,
        parts,
    })
}
