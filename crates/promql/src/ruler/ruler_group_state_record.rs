use super::*;

/// Rebuildable state for one ruler group evaluation.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RulerGroupStateRecord {
    pub tenant: String,
    pub namespace: String,
    pub group: String,
    pub last_eval_ms: i64,
}
