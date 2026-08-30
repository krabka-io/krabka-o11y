use super::Value;

pub(crate) struct PrometheusRuleGroupResponse {
    pub(crate) token: String,
    pub(crate) value: Value,
}
