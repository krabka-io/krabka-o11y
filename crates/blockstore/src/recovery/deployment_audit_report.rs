use super::{AuditReport, BTreeMap, DeploymentCut, Deserialize, Serialize};

/// Read-only verification of a complete deployment backup set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeploymentAuditReport {
    pub cut: DeploymentCut,
    pub cut_sha256: String,
    pub parts: BTreeMap<String, AuditReport>,
}
