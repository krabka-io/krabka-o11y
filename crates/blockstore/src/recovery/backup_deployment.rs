use super::{
    Arc, BTreeMap, BrokerState, COPY_CHUNK_BYTES, CUT_MANIFEST_PATH, CUT_SCHEMA_VERSION, CutPart,
    DeploymentBackupPlan, DeploymentBackupReport, DeploymentCut, ObjectPath, ObjectStore,
    RecoveryError, check_drained_groups, compare_snapshots, create_part_backup, digest,
    get_optional, json_bytes, load_backup_manifest_with_digest, part_store, put_create_or_equal,
    refuse_broker_findings, refuse_unplanned_objects, validate_cut, validate_plan,
};

/// Copies every part of a quiesced deployment and seals the set with one
/// [`DeploymentCut`].
///
/// The broker is read before and after the copy. The cut is written only
/// when the two reads are equal and every group in
/// [`DeploymentBackupPlan::drained_groups`] has no record after its committed
/// offset. A writer that moves during the copy therefore leaves an unsealed
/// set, which a restore refuses. A retry against the same backup root resumes
/// matching objects; a retry after the broker moved needs a new backup root.
///
/// # Errors
///
/// Returns [`RecoveryError::BrokerMismatch`] when a drained group lags its
/// topic or the broker moved during the copy, [`RecoveryError::MixedSet`]
/// when the backup root holds objects outside the planned parts or a
/// different sealed cut, and the errors of a part backup.
pub async fn backup_deployment(
    broker: &dyn BrokerState,
    backup: &Arc<dyn ObjectStore>,
    plan: &DeploymentBackupPlan,
) -> Result<DeploymentBackupReport, RecoveryError> {
    validate_plan(plan)?;
    if get_optional(backup.as_ref(), CUT_MANIFEST_PATH)
        .await?
        .is_some()
    {
        return Err(RecoveryError::MixedSet(
            "the backup root already holds a sealed cut".into(),
        ));
    }
    let names = plan
        .parts
        .iter()
        .map(|part| part.name.clone())
        .collect::<Vec<_>>();
    refuse_unplanned_objects(backup.as_ref(), &names).await?;

    let before = broker
        .capture()
        .await
        .map_err(RecoveryError::Broker)?
        .sorted();
    refuse_broker_findings(
        "backup",
        check_drained_groups(broker, &before, &plan.drained_groups).await?,
    )?;

    let mut reports = BTreeMap::new();
    let mut parts = Vec::with_capacity(plan.parts.len());
    for part in &plan.parts {
        let target = part_store(backup, &part.name);
        let report = create_part_backup(
            part.store.as_ref(),
            &target,
            None,
            Some(part.name.clone()),
            plan.cut_id.clone(),
            before.wal_offsets.clone(),
            COPY_CHUNK_BYTES,
        )
        .await?;
        let (manifest, manifest_sha256) = load_backup_manifest_with_digest(&target)
            .await?
            .ok_or_else(|| {
                RecoveryError::InvalidManifest(format!("part `{}` has no manifest", part.name))
            })?;
        parts.push(CutPart {
            name: part.name.clone(),
            manifest_sha256,
            object_count: manifest.objects.len(),
            byte_count: manifest.objects.iter().map(|object| object.size).sum(),
        });
        reports.insert(part.name.clone(), report);
    }

    let after = broker
        .capture()
        .await
        .map_err(RecoveryError::Broker)?
        .sorted();
    refuse_broker_findings("backup", compare_snapshots(&before, &after))?;

    parts.sort();
    let cut = DeploymentCut {
        schema_version: CUT_SCHEMA_VERSION,
        cut_id: plan.cut_id.clone(),
        broker_capture: plan.broker_capture.clone(),
        broker: before,
        parts,
    };
    validate_cut(&cut)?;
    let bytes = json_bytes(&cut)?;
    let cut_sha256 = digest(&bytes);
    let _ =
        put_create_or_equal(backup.as_ref(), &ObjectPath::from(CUT_MANIFEST_PATH), bytes).await?;
    Ok(DeploymentBackupReport {
        cut,
        cut_sha256,
        parts: reports,
    })
}
