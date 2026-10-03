use super::{
    Arc, BTreeMap, ObjectMeta, ObjectStore, ObjectStoreExt, RepairAction, RepairLogEntry,
    RepairLogWriter, RepairOptions, RepairOutcome, RepairReport, StorageAuditError, StorageFinding,
    UNIX_EPOCH, audit_inventory, instrument, is_older_than_grace,
};

/// Deletes the orphans that an audit of one tenant finds, and nothing else.
///
/// The repair first validates the scope, then audits the tenant. It acts
/// only on findings of an allowed kind whose tenant is exactly the requested
/// tenant. Without `apply` it stops there and reports the plan.
///
/// With `apply`, the repair reads the head of each object again before it
/// deletes it. It leaves the object when the entity tag or the modification
/// time differ from what the audit listed, or when the object is no longer
/// older than the grace window. `object_store` has no conditional
/// delete, so the grace window is what keeps a live writer safe, as it does
/// for [`reconcile_orphans`](crate::reconcile_orphans).
///
/// Each action goes to `audit_log` as one `outcome` line, synced before the
/// next action. Before each delete, the repair writes and syncs an `intent`
/// line for the object, and it starts the delete only after that. A crash
/// after the delete therefore leaves the `intent` line as the record of it.
/// A second run finds nothing left to do, and a run that stopped half way
/// continues where it stopped.
///
/// # Errors
/// Returns [`StorageAuditError::InvalidScope`] for a scope that
/// [`RepairOptions::validate`] refuses, any error of
/// [`audit_store`](super::audit_store), and
/// [`StorageAuditError::AuditLog`] when the log does not take a line. The
/// repair stops at that line. When the line is an `intent` line, the repair
/// did not delete the object. A failed delete is not an error: the report
/// counts it as `failed`.
#[instrument(level = "info", skip_all, err, fields(tenant = %options.tenant, signal = %options.signal))]
pub async fn repair_store(
    store: &Arc<dyn ObjectStore>,
    options: &RepairOptions,
    audit_log: &mut dyn RepairLogWriter,
) -> Result<RepairReport, StorageAuditError> {
    options.validate()?;
    let audit_options = options.audit_options();
    let (audit, inventory) = audit_inventory(store, &audit_options).await?;
    let planned = in_scope(&audit.findings, options);
    let started = options
        .now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let mut actions = Vec::with_capacity(planned.len());
    for (path, finding) in &planned {
        let action = if options.apply {
            apply_one(
                store,
                finding,
                inventory.meta(path),
                options,
                started,
                audit_log,
            )
            .await?
        } else {
            RepairAction {
                kind: finding.kind,
                path: path.clone(),
                outcome: RepairOutcome::Planned,
                detail: None,
            }
        };
        let entry = RepairLogEntry::new(
            started,
            options.apply,
            &options.tenant,
            options.signal,
            &action,
        );
        write_log_line(audit_log, &entry)?;
        actions.push(action);
    }
    Ok(RepairReport::new(
        options.apply,
        options.tenant.clone(),
        options.signal,
        options.kinds.iter().copied().collect(),
        audit.findings.len(),
        actions,
    ))
}

fn in_scope(
    findings: &[StorageFinding],
    options: &RepairOptions,
) -> BTreeMap<String, StorageFinding> {
    findings
        .iter()
        .filter(|finding| {
            finding.repairable
                && finding.signal == options.signal
                && options.kinds.contains(&finding.kind)
                && finding.tenant.as_deref() == Some(options.tenant.as_str())
        })
        .map(|finding| (finding.path.clone(), finding.clone()))
        .collect()
}

async fn apply_one(
    store: &Arc<dyn ObjectStore>,
    finding: &StorageFinding,
    listed: Option<&ObjectMeta>,
    options: &RepairOptions,
    started: u64,
    audit_log: &mut dyn RepairLogWriter,
) -> Result<RepairAction, StorageAuditError> {
    let (outcome, detail) = match listed {
        None => (RepairOutcome::AlreadyAbsent, None),
        Some(listed) => {
            if let Err(skipped) = recheck(store, listed, options).await {
                skipped
            } else {
                let intent = RepairLogEntry::intent(
                    started,
                    &options.tenant,
                    options.signal,
                    finding.kind,
                    &finding.path,
                );
                write_log_line(audit_log, &intent)?;
                delete(store, listed).await
            }
        }
    };
    Ok(RepairAction {
        kind: finding.kind,
        path: finding.path.clone(),
        outcome,
        detail,
    })
}

/// Reads the head of the object again. Returns `Ok` when the object is as
/// the audit listed it and old enough to delete, and the outcome to record
/// otherwise.
async fn recheck(
    store: &Arc<dyn ObjectStore>,
    listed: &ObjectMeta,
    options: &RepairOptions,
) -> Result<(), (RepairOutcome, Option<String>)> {
    let current = match store.head(&listed.location).await {
        Ok(current) => current,
        Err(object_store::Error::NotFound { .. }) => {
            return Err((RepairOutcome::AlreadyAbsent, None));
        }
        Err(error) => return Err((RepairOutcome::Failed, Some(error.to_string()))),
    };
    if current.e_tag != listed.e_tag || current.last_modified != listed.last_modified {
        return Err((
            RepairOutcome::SkippedChanged,
            Some("the object changed after the audit".to_string()),
        ));
    }
    if !is_older_than_grace(&current, options.grace, options.now) {
        return Err((
            RepairOutcome::SkippedChanged,
            Some("the object is inside the grace window".to_string()),
        ));
    }
    Ok(())
}

async fn delete(
    store: &Arc<dyn ObjectStore>,
    listed: &ObjectMeta,
) -> (RepairOutcome, Option<String>) {
    match store.delete(&listed.location).await {
        Ok(()) => (RepairOutcome::Deleted, None),
        Err(object_store::Error::NotFound { .. }) => (RepairOutcome::AlreadyAbsent, None),
        Err(error) => (RepairOutcome::Failed, Some(error.to_string())),
    }
}

fn write_log_line(
    audit_log: &mut dyn RepairLogWriter,
    entry: &RepairLogEntry,
) -> Result<(), StorageAuditError> {
    let mut line = serde_json::to_vec(entry)
        .map_err(|error| StorageAuditError::AuditLog(error.to_string()))?;
    line.push(b'\n');
    audit_log
        .write_all(&line)
        .and_then(|()| audit_log.sync())
        .map_err(|error| StorageAuditError::AuditLog(error.to_string()))
}
