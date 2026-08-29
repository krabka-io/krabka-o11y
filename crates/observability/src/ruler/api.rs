#[allow(clippy::wildcard_imports)]
use super::*;

#[path = "api/loki_api.rs"]
pub(crate) mod loki_api;
pub(crate) use loki_api::*;
#[path = "api/prometheus_rules.rs"]
pub(crate) mod prometheus_rules;
pub(crate) use prometheus_rules::*;
#[path = "api/prometheus_alerts.rs"]
pub(crate) mod prometheus_alerts;
pub(crate) use prometheus_alerts::*;
