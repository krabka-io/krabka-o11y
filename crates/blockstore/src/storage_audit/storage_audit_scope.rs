use super::{Deserialize, Serialize, StorageSignal};

/// What an audit looked at, recorded in its report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageAuditScope {
    /// The one signal audited, or `None` for all four.
    pub signal: Option<StorageSignal>,
    /// The one tenant audited, or `None` for every tenant.
    pub tenant: Option<String>,
    /// The grace window in whole seconds. An unindexed object younger than
    /// this is `pending`, not `orphan`.
    pub grace_secs: u64,
    /// Whether the audit read every block in full, not only its footer.
    pub verify_data: bool,
}
