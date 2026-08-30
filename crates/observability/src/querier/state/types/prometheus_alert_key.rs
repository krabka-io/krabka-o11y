use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct PrometheusAlertKey {
    pub(crate) tenant: String,
    pub(crate) alert_name: String,
    pub(crate) query: String,
    pub(crate) labels: Labels,
}
