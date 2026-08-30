use super::*;

#[derive(Default)]
pub(crate) struct PrometheusRulesPage {
    pub(crate) groups: Vec<Value>,
    pub(crate) next_token: Option<String>,
}
