use super::{Deserialize, Serialize};

/// What a repair did with one finding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairOutcome {
    /// The repair ran without `apply` and would delete the object.
    Planned,
    /// The repair deleted the object.
    Deleted,
    /// The object was gone when the repair reached it. A retention pass or
    /// an earlier repair run removed it.
    AlreadyAbsent,
    /// The object changed after the audit, or a second audit no longer
    /// reports it. A writer may still publish it, so the repair left it.
    SkippedChanged,
    /// The recheck or the delete failed. The object stays, and the next run
    /// reaches it again.
    Failed,
}
