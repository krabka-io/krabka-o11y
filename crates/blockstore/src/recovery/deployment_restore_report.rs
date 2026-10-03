use super::{BTreeMap, BrokerSnapshot, DeploymentCut, Deserialize, RestoreReport, Serialize};

/// Result of a deployment restore.
///
/// `broker` is the restored broker state. A restore writes no object unless
/// it equals the state that the cut recorded.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeploymentRestoreReport {
    pub cut: DeploymentCut,
    pub cut_sha256: String,
    pub broker: BrokerSnapshot,
    pub parts: BTreeMap<String, RestoreReport>,
}
