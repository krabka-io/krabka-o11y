use super::{
    BTreeMap, Deserialize, RepairAction, RepairOutcome, STORAGE_AUDIT_SCHEMA_VERSION, Serialize,
    StorageFindingKind, StorageSignal,
};

/// The machine-readable result of one repair run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairReport {
    /// [`STORAGE_AUDIT_SCHEMA_VERSION`] of the build that wrote the report.
    pub schema_version: u32,
    /// Whether the run deleted, or only planned.
    pub applied: bool,
    pub tenant: String,
    pub signal: StorageSignal,
    pub kinds: Vec<StorageFindingKind>,
    /// Findings of the audit the repair ran first, of every kind.
    pub findings_audited: usize,
    /// The findings in scope, in report order.
    pub actions: Vec<RepairAction>,
    /// The number of actions with each outcome. An outcome with no action is
    /// absent.
    pub counts: BTreeMap<RepairOutcome, usize>,
}

impl RepairReport {
    /// A report over `actions`, with the counts filled in.
    #[must_use]
    pub fn new(
        applied: bool,
        tenant: String,
        signal: StorageSignal,
        kinds: Vec<StorageFindingKind>,
        findings_audited: usize,
        actions: Vec<RepairAction>,
    ) -> Self {
        let mut counts = BTreeMap::new();
        for action in &actions {
            *counts.entry(action.outcome).or_insert(0) += 1;
        }
        Self {
            schema_version: STORAGE_AUDIT_SCHEMA_VERSION,
            applied,
            tenant,
            signal,
            kinds,
            findings_audited,
            actions,
            counts,
        }
    }

    /// Whether any recheck or delete failed.
    #[must_use]
    pub fn has_failures(&self) -> bool {
        self.counts.contains_key(&RepairOutcome::Failed)
    }
}
