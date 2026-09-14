use std::collections::BTreeSet;

use super::Deserialize;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RulesParams {
    #[serde(rename = "type")]
    pub(crate) rule_type: Option<String>,
    pub(crate) exclude_alerts: Option<bool>,
    pub(crate) rule_names: BTreeSet<String>,
    pub(crate) rule_groups: BTreeSet<String>,
    pub(crate) files: BTreeSet<String>,
    pub(crate) group_limit: Option<usize>,
    pub(crate) group_next_token: Option<String>,
}
