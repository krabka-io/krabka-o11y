use super::{
    Deserialize, RepairAction, RepairLogPhase, RepairOutcome, STORAGE_AUDIT_SCHEMA_VERSION,
    Serialize, StorageAuditError, StorageFindingKind, StorageSignal,
};

/// One line of the repair audit log.
///
/// The log is JSON Lines. A repair appends one `outcome` line per action, in
/// the order it acts, and syncs each line before it moves on. Before a
/// delete, it also appends and syncs an `intent` line for the object. The
/// delete starts only after that sync. A run that stopped half way therefore
/// leaves an `intent` line for every object it may have deleted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairLogEntry {
    /// [`STORAGE_AUDIT_SCHEMA_VERSION`] of the build that wrote the line.
    pub schema_version: u32,
    /// The start of the run, in whole seconds since the Unix epoch.
    pub run_started_at_secs: u64,
    pub applied: bool,
    pub tenant: String,
    pub signal: StorageSignal,
    pub phase: RepairLogPhase,
    pub kind: StorageFindingKind,
    pub path: String,
    /// What the repair did. `None` on an `intent` line.
    pub outcome: Option<RepairOutcome>,
    pub detail: Option<String>,
}

impl RepairLogEntry {
    /// The `outcome` line for `action`.
    #[must_use]
    pub fn new(
        run_started_at_secs: u64,
        applied: bool,
        tenant: &str,
        signal: StorageSignal,
        action: &RepairAction,
    ) -> Self {
        Self {
            schema_version: STORAGE_AUDIT_SCHEMA_VERSION,
            run_started_at_secs,
            applied,
            tenant: tenant.to_string(),
            signal,
            phase: RepairLogPhase::Outcome,
            kind: action.kind,
            path: action.path.clone(),
            outcome: Some(action.outcome),
            detail: action.detail.clone(),
        }
    }

    /// The `intent` line that an applied repair writes before it deletes
    /// `path`.
    #[must_use]
    pub fn intent(
        run_started_at_secs: u64,
        tenant: &str,
        signal: StorageSignal,
        kind: StorageFindingKind,
        path: &str,
    ) -> Self {
        Self {
            schema_version: STORAGE_AUDIT_SCHEMA_VERSION,
            run_started_at_secs,
            applied: true,
            tenant: tenant.to_string(),
            signal,
            phase: RepairLogPhase::Intent,
            kind,
            path: path.to_string(),
            outcome: None,
            detail: None,
        }
    }

    /// Decodes one log line and refuses one from another schema version.
    ///
    /// # Errors
    /// Returns [`StorageAuditError::UnsupportedSchemaVersion`] for a line of
    /// another version, and [`StorageAuditError::InvalidReport`] for a line
    /// that is not an entry.
    pub fn from_json_line(line: &str) -> Result<Self, StorageAuditError> {
        let entry: Self = serde_json::from_str(line)
            .map_err(|error| StorageAuditError::InvalidReport(error.to_string()))?;
        if entry.schema_version != STORAGE_AUDIT_SCHEMA_VERSION {
            return Err(StorageAuditError::UnsupportedSchemaVersion {
                actual: entry.schema_version,
                expected: STORAGE_AUDIT_SCHEMA_VERSION,
            });
        }
        Ok(entry)
    }
}
