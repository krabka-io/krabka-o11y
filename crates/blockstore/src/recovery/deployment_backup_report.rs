use super::{BTreeMap, DeploymentCut, Deserialize, RestoreReport, Serialize};

/// Result of a deployment backup: the sealed cut and the pass over each part.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeploymentBackupReport {
    pub cut: DeploymentCut,
    pub cut_sha256: String,
    pub parts: BTreeMap<String, RestoreReport>,
}
