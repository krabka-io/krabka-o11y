use super::{
    Deserialize, RepairAction, RepairOutcome, STORAGE_AUDIT_SCHEMA_VERSION, Serialize,
    StorageAuditError, StorageFindingKind, StorageSignal,
};

/// One line of the repair audit log.
///
/// The log is JSON Lines. A repair appends one line per action, in the order
/// it acts, and flushes each line before it moves on. The log of a run that
/// stopped half way therefore says exactly which objects that run deleted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairLogEntry {
    /// [`STORAGE_AUDIT_SCHEMA_VERSION`] of the build that wrote the line.
    pub schema_version: u32,
    /// The start of the run, in whole seconds since the Unix epoch.
    pub run_started_at_secs: u64,
    pub applied: bool,
    pub tenant: String,
    pub signal: StorageSignal,
    pub kind: StorageFindingKind,
    pub path: String,
    pub outcome: RepairOutcome,
    pub detail: Option<String>,
}

impl RepairLogEntry {
    /// The log line for `action`.
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
            kind: action.kind,
            path: action.path.clone(),
            outcome: action.outcome,
            detail: action.detail.clone(),
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
