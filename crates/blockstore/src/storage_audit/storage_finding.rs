use super::{Deserialize, Serialize, StorageFindingKind, StorageFindingSeverity, StorageSignal};

/// One thing an audit found wrong with one object.
///
/// Findings sort by kind, then signal, tenant, path and detail, so two audits
/// of one store list them in one order.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct StorageFinding {
    pub kind: StorageFindingKind,
    pub signal: StorageSignal,
    /// The tenant that owns the object. `None` marks shared state, such as a
    /// fleet-wide index or the logs compaction frontier.
    pub tenant: Option<String>,
    /// The object key, relative to the audited prefix.
    pub path: String,
    /// What the audit saw, for a person to read. Not a stable contract.
    pub detail: String,
    /// Copied from the kind, so a reader of the report does not need a table.
    pub severity: StorageFindingSeverity,
    /// Copied from the kind. Only a repairable finding can be deleted, and
    /// only when a repair names its kind.
    pub repairable: bool,
}

impl StorageFinding {
    /// A finding of `kind`, with the severity and repairability that `kind`
    /// implies.
    #[must_use]
    pub fn new(
        kind: StorageFindingKind,
        signal: StorageSignal,
        tenant: Option<String>,
        path: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            signal,
            tenant,
            path: path.into(),
            detail: detail.into(),
            severity: kind.severity(),
            repairable: kind.is_repairable(),
        }
    }
}
