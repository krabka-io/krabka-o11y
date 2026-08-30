use super::BTreeMap;

/// Rebuildable state for one alert instance.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RulerAlertStateRecord {
    pub tenant: String,
    pub rule_id: String,
    pub labels: BTreeMap<String, String>,
    pub active_since_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_firing_until_ms: Option<i64>,
}
