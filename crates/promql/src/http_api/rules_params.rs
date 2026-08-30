use super::*;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RulesParams {
    #[serde(rename = "type")]
    pub(crate) rule_type: Option<String>,
    pub(crate) exclude_alerts: Option<bool>,
}
