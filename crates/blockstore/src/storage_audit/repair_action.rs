use super::{Deserialize, RepairOutcome, Serialize, StorageFindingKind};

/// One object a repair acted on.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairAction {
    pub kind: StorageFindingKind,
    pub path: String,
    pub outcome: RepairOutcome,
    /// Why the repair skipped the object or failed, for a person to read.
    pub detail: Option<String>,
}
