use super::*;

#[derive(Debug, Default, PartialEq)]
pub(crate) struct PrometheusRulesFilters {
    pub(crate) rule_kind: Option<&'static str>,
    pub(crate) rule_names: BTreeSet<String>,
    pub(crate) rule_groups: BTreeSet<String>,
    pub(crate) files: BTreeSet<String>,
    pub(crate) label_selectors: Vec<StreamQuery>,
    pub(crate) group_limit: Option<usize>,
    pub(crate) group_next_token: Option<String>,
    pub(crate) exclude_alerts: bool,
    pub(crate) evaluation_time: Option<i64>,
}
